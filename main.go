package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	baseURL        = "https://fapi.binance.com"
	exchangeInfoEP = "/fapi/v1/exchangeInfo"
	ticker24hrEP   = "/fapi/v1/ticker/24hr"
	klinesEP       = "/fapi/v1/klines"
	wsBaseURL      = "wss://fstream.binance.com/ws"
)

const (
	initialKlineLimit  = 499
	positionKlineLimit = 499
	max1mCandles       = 2000
)

type exchangeInfoResp struct {
	Symbols []struct {
		Symbol       string `json:"symbol"`
		Status       string `json:"status"`
		QuoteAsset   string `json:"quoteAsset"`
		ContractType string `json:"contractType"`
	} `json:"symbols"`
}

type ticker24hr struct {
	Symbol      string `json:"symbol"`
	Volume      string `json:"volume"`
	QuoteVolume string `json:"quoteVolume"`
}

type symbolMetrics struct {
	Symbol       string
	Change10m    float64
	Change30m    float64
	Change60m    float64
	Change120m   float64
	Last         float64
	AvgAmplitude float64
}

type candle struct {
	OpenTime  time.Time
	CloseTime time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

type timeframeAnalysis struct {
	Interval     string
	EMAFast      float64
	EMASlow      float64
	EMAFastSlope float64
	EMASlowSlope float64
	MACDHist     float64
	MACDPrev     float64
	ADX          float64
	LongSignal   bool
	ShortSignal  bool
}

type strategyResult struct {
	Symbol      string
	Metrics     symbolMetrics
	ThreeMinute timeframeAnalysis
	FiveMinute  timeframeAnalysis
	LongSignal  bool
	ShortSignal bool
	Score       float64
}

type symbolWatcher struct {
	symbol   string
	mu       sync.RWMutex
	candles  []candle
	cancel   context.CancelFunc
	done     chan struct{}
	onCandle func(string)
}

type symbolManager struct {
	mu       sync.RWMutex
	watchers map[string]*symbolWatcher
	client   *http.Client
	cfg      config
	onCandle func(string)
}

type symbolFilter struct {
	stepSize float64
	minQty   float64
}

type wsKlineEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Kline     struct {
		StartTime int64  `json:"t"`
		CloseTime int64  `json:"T"`
		Interval  string `json:"i"`
		Open      string `json:"o"`
		Close     string `json:"c"`
		High      string `json:"h"`
		Low       string `json:"l"`
		Volume    string `json:"v"`
		Final     bool   `json:"x"`
	} `json:"k"`
}

func (e wsKlineEvent) toCandle() (candle, error) {
	var c candle
	if !e.Kline.Final {
		return c, errors.New("kline not closed")
	}
	op, err := strconv.ParseFloat(e.Kline.Open, 64)
	if err != nil {
		return c, err
	}
	cl, err := strconv.ParseFloat(e.Kline.Close, 64)
	if err != nil {
		return c, err
	}
	h, err := strconv.ParseFloat(e.Kline.High, 64)
	if err != nil {
		return c, err
	}
	l, err := strconv.ParseFloat(e.Kline.Low, 64)
	if err != nil {
		return c, err
	}
	vol, err := strconv.ParseFloat(e.Kline.Volume, 64)
	if err != nil {
		return c, err
	}
	openTime := time.UnixMilli(e.Kline.StartTime)
	closeTime := time.UnixMilli(e.Kline.CloseTime)
	c = candle{
		OpenTime:  openTime,
		CloseTime: closeTime,
		Open:      op,
		High:      h,
		Low:       l,
		Close:     cl,
		Volume:    vol,
	}
	return c, nil
}

func newSymbolManager(client *http.Client, cfg config) *symbolManager {
	return &symbolManager{
		watchers: make(map[string]*symbolWatcher),
		client:   client,
		cfg:      cfg,
		onCandle: nil,
	}
}

func (m *symbolManager) EnsureWatchers(ctx context.Context, symbols []string) {
	for _, sym := range symbols {
		sym = strings.ToUpper(sym)
		if sym == "" {
			continue
		}
		m.mu.RLock()
		wExisting, exists := m.watchers[sym]
		m.mu.RUnlock()
		if exists {
			wExisting.setHandler(m.onCandle)
			continue
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
		candles, err := fetchHistorical1m(ctxTimeout, m.client, sym, initialKlineLimit)
		cancel()
		if err != nil {
			log.Printf("初始化 %s 监听失败: %v", sym, err)
			continue
		}

		watcher := newSymbolWatcher(sym, candles, m.onCandle)

		m.mu.Lock()
		if _, exists := m.watchers[sym]; exists {
			m.mu.Unlock()
			continue
		}
		m.watchers[sym] = watcher
		m.mu.Unlock()

		watcher.start(ctx)
		log.Printf("开始监听 %s，初始1m K线: %d 条", sym, len(candles))
	}
}

func (m *symbolManager) getWatcher(symbol string) *symbolWatcher {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.watchers[strings.ToUpper(symbol)]
}

func (m *symbolManager) SetCandleHandler(handler func(string)) {
	m.mu.Lock()
	m.onCandle = handler
	for _, w := range m.watchers {
		w.setHandler(handler)
	}
	m.mu.Unlock()
}

func (m *symbolManager) EvaluateStrategies(symbols []string) []strategyResult {
	results := make([]strategyResult, 0, len(symbols))
	for _, sym := range symbols {
		w := m.getWatcher(sym)
		if w == nil {
			continue
		}
		candles := w.snapshot()
		if len(candles) == 0 {
			continue
		}
		rs, err := computeStrategyFromCandles(sym, candles, m.cfg)
		if err != nil {
			log.Printf("策略计算失败 %s: %v", sym, err)
			continue
		}
		results = append(results, rs)
	}
	return results
}

func (m *symbolManager) Close() {
	m.mu.Lock()
	watchers := make([]*symbolWatcher, 0, len(m.watchers))
	for _, w := range m.watchers {
		watchers = append(watchers, w)
	}
	m.watchers = make(map[string]*symbolWatcher)
	m.mu.Unlock()
	for _, w := range watchers {
		w.Close()
	}
}

func dedupeCandles(candles []candle) []candle {
	if len(candles) < 2 {
		return candles
	}
	result := make([]candle, 0, len(candles))
	for _, c := range candles {
		if len(result) == 0 {
			result = append(result, c)
			continue
		}
		last := &result[len(result)-1]
		if last.OpenTime.Equal(c.OpenTime) {
			*last = c
			continue
		}
		if c.OpenTime.After(last.OpenTime) {
			result = append(result, c)
		}
	}
	return result
}

func newSymbolWatcher(symbol string, candles []candle, handler func(string)) *symbolWatcher {
	clean := dedupeCandles(candles)
	copyCandles := make([]candle, len(clean))
	copy(copyCandles, clean)
	return &symbolWatcher{
		symbol:   symbol,
		candles:  copyCandles,
		done:     make(chan struct{}),
		onCandle: handler,
	}
}

func (w *symbolWatcher) setHandler(handler func(string)) {
	w.mu.Lock()
	w.onCandle = handler
	w.mu.Unlock()
}

func (w *symbolWatcher) start(ctx context.Context) {
	childCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	go w.run(childCtx)
}

func (w *symbolWatcher) run(ctx context.Context) {
	defer close(w.done)
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := w.connect(ctx)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("监听 %s 连接断开: %v", w.symbol, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (w *symbolWatcher) connect(ctx context.Context) error {
	endpoint := fmt.Sprintf("%s/%s@kline_1m", wsBaseURL, strings.ToLower(w.symbol))
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var evt wsKlineEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			continue
		}
		if !evt.Kline.Final {
			continue
		}
		c, err := evt.toCandle()
		if err != nil {
			continue
		}
		w.upsert(c)
	}
}

func (w *symbolWatcher) upsert(c candle) {
	w.mu.Lock()
	handler := w.onCandle
	updated := false
	n := len(w.candles)
	if n > 0 && w.candles[n-1].OpenTime.Equal(c.OpenTime) {
		w.candles[n-1] = c
		updated = true
	} else {
		w.candles = append(w.candles, c)
		if len(w.candles) > max1mCandles {
			w.candles = w.candles[len(w.candles)-max1mCandles:]
		}
		updated = true
	}
	w.mu.Unlock()
	if updated && handler != nil {
		go handler(w.symbol)
	}
}

func (w *symbolWatcher) snapshot() []candle {
	w.mu.RLock()
	defer w.mu.RUnlock()
	copyCandles := make([]candle, len(w.candles))
	copy(copyCandles, w.candles)
	return dedupeCandles(copyCandles)
}

func (w *symbolWatcher) Close() {
	if w.cancel != nil {
		w.cancel()
	}
	<-w.done
}

type binanceClient struct {
	baseURL       string
	client        *http.Client
	apiKey        string
	secret        []byte
	recvWindow    int
	settingsMu    sync.Mutex
	configuredSym map[string]struct{}
	filtersMu     sync.RWMutex
	filters       map[string]symbolFilter
}

func newBinanceClient(httpClient *http.Client, cfg config) *binanceClient {
	return newBinanceClientWithURL(httpClient, cfg, baseURL)
}

func newBinanceClientWithURL(httpClient *http.Client, cfg config, url string) *binanceClient {
	return &binanceClient{
		baseURL:       url,
		client:        httpClient,
		apiKey:        cfg.apiKey,
		secret:        []byte(cfg.apiSecret),
		recvWindow:    cfg.recvWindow,
		configuredSym: make(map[string]struct{}),
		filters:       make(map[string]symbolFilter),
	}
}

func (c *binanceClient) sign(query string) string {
	mac := hmac.New(sha256.New, c.secret)
	mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *binanceClient) signedRequest(ctx context.Context, method, path string, params url.Values) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("timestamp", fmt.Sprintf("%d", time.Now().UnixMilli()))
	params.Set("recvWindow", strconv.Itoa(c.recvWindow))
	query := params.Encode()
	signature := c.sign(query)
	params.Set("signature", signature)
	finalQuery := params.Encode()

	var body io.Reader
	endpoint := c.baseURL + path
	if method == http.MethodGet || method == http.MethodDelete {
		if strings.Contains(endpoint, "?") {
			endpoint = endpoint + "&" + finalQuery
		} else {
			endpoint = endpoint + "?" + finalQuery
		}
	} else {
		body = strings.NewReader(finalQuery)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("binance api error %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return bodyBytes, nil
}

func (c *binanceClient) publicRequest(ctx context.Context, path string, params url.Values) ([]byte, error) {
	endpoint := c.baseURL + path
	if params != nil && len(params) > 0 {
		if strings.Contains(endpoint, "?") {
			endpoint = endpoint + "&" + params.Encode()
		} else {
			endpoint = endpoint + "?" + params.Encode()
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("binance public api error %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func (c *binanceClient) EnsureDualSide(ctx context.Context) error {
	params := url.Values{}
	params.Set("dualSidePosition", "true")
	_, err := c.signedRequest(ctx, http.MethodPost, "/fapi/v1/positionSide/dual", params)
	if err != nil && !strings.Contains(err.Error(), "-4059") {
		return err
	}
	return nil
}

func (c *binanceClient) FetchWalletBalance(ctx context.Context) (float64, error) {
	body, err := c.signedRequest(ctx, http.MethodGet, "/fapi/v2/account", nil)
	if err != nil {
		return 0, err
	}
	var payload struct {
		TotalWalletBalance string `json:"totalWalletBalance"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, err
	}
	bal, err := strconv.ParseFloat(payload.TotalWalletBalance, 64)
	if err != nil {
		return 0, err
	}
	return bal, nil
}

func (c *binanceClient) EnsureSymbolSettings(ctx context.Context, symbol string, leverage int) error {
	c.settingsMu.Lock()
	if _, ok := c.configuredSym[symbol]; ok {
		c.settingsMu.Unlock()
		return nil
	}
	c.settingsMu.Unlock()

	if err := c.setMarginType(ctx, symbol, "CROSSED"); err != nil {
		if !strings.Contains(err.Error(), "-4046") { // Margin type already set
			return err
		}
	}
	if err := c.setLeverage(ctx, symbol, leverage); err != nil {
		return err
	}

	c.settingsMu.Lock()
	c.configuredSym[symbol] = struct{}{}
	c.settingsMu.Unlock()
	return nil
}

func (c *binanceClient) setMarginType(ctx context.Context, symbol, marginType string) error {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("marginType", marginType)
	_, err := c.signedRequest(ctx, http.MethodPost, "/fapi/v1/marginType", params)
	return err
}

func (c *binanceClient) setLeverage(ctx context.Context, symbol string, leverage int) error {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("leverage", strconv.Itoa(leverage))
	_, err := c.signedRequest(ctx, http.MethodPost, "/fapi/v1/leverage", params)
	return err
}

func (c *binanceClient) getSymbolFilter(ctx context.Context, symbol string) (symbolFilter, error) {
	symbol = strings.ToUpper(symbol)
	c.filtersMu.RLock()
	if f, ok := c.filters[symbol]; ok {
		c.filtersMu.RUnlock()
		return f, nil
	}
	c.filtersMu.RUnlock()

	params := url.Values{}
	params.Set("symbol", symbol)
	body, err := c.publicRequest(ctx, "/fapi/v1/exchangeInfo", params)
	if err != nil {
		return symbolFilter{}, err
	}
	var resp struct {
		Symbols []struct {
			Symbol  string `json:"symbol"`
			Filters []struct {
				FilterType string `json:"filterType"`
				StepSize   string `json:"stepSize"`
				MinQty     string `json:"minQty"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return symbolFilter{}, err
	}
	var lot symbolFilter
	for _, sym := range resp.Symbols {
		if strings.EqualFold(sym.Symbol, symbol) {
			for _, flt := range sym.Filters {
				switch flt.FilterType {
				case "MARKET_LOT_SIZE", "LOT_SIZE":
					step, err := strconv.ParseFloat(flt.StepSize, 64)
					if err != nil {
						continue
					}
					min, err := strconv.ParseFloat(flt.MinQty, 64)
					if err != nil {
						continue
					}
					lot.stepSize = step
					lot.minQty = min
				}
			}
			break
		}
	}
	if lot.stepSize <= 0 {
		return symbolFilter{}, fmt.Errorf("未获取到 %s 的步长限制", symbol)
	}
	c.filtersMu.Lock()
	c.filters[symbol] = lot
	c.filtersMu.Unlock()
	return lot, nil
}

func (c *binanceClient) AdjustQuantity(ctx context.Context, symbol string, qty float64) (float64, error) {
	if qty <= 0 {
		return 0, fmt.Errorf("下单数量无效")
	}
	filter, err := c.getSymbolFilter(ctx, symbol)
	if err != nil {
		return 0, err
	}
	if filter.stepSize <= 0 {
		return 0, fmt.Errorf("%s 步长限制无效", symbol)
	}
	step := filter.stepSize
	adjusted := math.Floor(qty/step) * step
	if adjusted < filter.minQty {
		return 0, fmt.Errorf("%s 数量 %.8f 低于最小下单量 %.8f", symbol, adjusted, filter.minQty)
	}
	return adjusted, nil
}

type orderRequest struct {
	Symbol       string
	Side         string
	PositionSide string
	Quantity     float64
}

func (c *binanceClient) PlaceOrder(ctx context.Context, req orderRequest) error {
	params := url.Values{}
	params.Set("symbol", req.Symbol)
	params.Set("side", req.Side)
	params.Set("type", "MARKET")
	params.Set("quantity", formatQuantity(req.Quantity))
	params.Set("positionSide", req.PositionSide)
	_, err := c.signedRequest(ctx, http.MethodPost, "/fapi/v1/order", params)
	return err
}

type positionEntry struct {
	Symbol string
	Side   string
	Qty    float64
}

func (c *binanceClient) FetchOpenPositions(ctx context.Context) ([]positionEntry, error) {
	body, err := c.signedRequest(ctx, http.MethodGet, "/fapi/v2/positionRisk", url.Values{})
	if err != nil {
		return nil, err
	}
	var payload []map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	positions := make([]positionEntry, 0)
	for _, item := range payload {
		symbol, _ := item["symbol"].(string)
		posSide, _ := item["positionSide"].(string)
		qtyStr, _ := item["positionAmt"].(string)
		if symbol == "" || posSide == "" || qtyStr == "" {
			continue
		}
		size, err := strconv.ParseFloat(qtyStr, 64)
		if err != nil {
			continue
		}
		if math.Abs(size) < 1e-8 {
			continue
		}
		if size < 0 {
			size = math.Abs(size)
		}
		positions = append(positions, positionEntry{Symbol: symbol, Side: posSide, Qty: size})
	}
	return positions, nil
}

func formatQuantity(q float64) string {
	return strconv.FormatFloat(q, 'f', -1, 64)
}

type positionKey struct {
	Symbol string
	Side   string
}

type positionManager struct {
	mu           sync.RWMutex
	positions    map[positionKey]positionEntry
	maxPositions int
}

func newPositionManager(max int) *positionManager {
	return &positionManager{
		positions:    make(map[positionKey]positionEntry),
		maxPositions: max,
	}
}

func (pm *positionManager) Count() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.positions)
}

func (pm *positionManager) Remaining() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if len(pm.positions) >= pm.maxPositions {
		return 0
	}
	return pm.maxPositions - len(pm.positions)
}

func (pm *positionManager) Has(symbol, side string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	_, ok := pm.positions[positionKey{Symbol: symbol, Side: side}]
	return ok
}

func (pm *positionManager) Get(symbol, side string) (positionEntry, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	val, ok := pm.positions[positionKey{Symbol: symbol, Side: side}]
	return val, ok
}

func (pm *positionManager) Set(sym, side string, qty float64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.positions[positionKey{Symbol: sym, Side: side}] = positionEntry{Symbol: sym, Side: side, Qty: qty}
}

func (pm *positionManager) Remove(sym, side string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.positions, positionKey{Symbol: sym, Side: side})
}

func (pm *positionManager) Replace(entries []positionEntry) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.positions = make(map[positionKey]positionEntry, len(entries))
	for _, e := range entries {
		pm.positions[positionKey{Symbol: e.Symbol, Side: e.Side}] = e
	}
}

func (pm *positionManager) Symbols() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if len(pm.positions) == 0 {
		return nil
	}
	syms := make([]string, 0, len(pm.positions))
	seen := make(map[string]struct{}, len(pm.positions))
	for key := range pm.positions {
		symbol := strings.ToUpper(key.Symbol)
		if symbol == "" {
			continue
		}
		if _, ok := seen[symbol]; ok {
			continue
		}
		seen[symbol] = struct{}{}
		syms = append(syms, symbol)
	}
	return syms
}

func (pm *positionManager) Entries() []positionEntry {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if len(pm.positions) == 0 {
		return nil
	}
	out := make([]positionEntry, 0, len(pm.positions))
	for _, entry := range pm.positions {
		out = append(out, entry)
	}
	return out
}

type tradeManager struct {
	client     *binanceClient
	positions  *positionManager
	cfg        config
	qtyMu      sync.RWMutex
	qty        map[string]float64
	httpClient *http.Client
	watchMgr   *symbolManager
}

func newTradeManager(client *binanceClient, positions []positionEntry, cfg config, httpClient *http.Client, watchMgr *symbolManager) *tradeManager {
	pm := newPositionManager(cfg.maxPositions)
	pm.Replace(positions)
	return &tradeManager{client: client, positions: pm, cfg: cfg, qty: make(map[string]float64), httpClient: httpClient, watchMgr: watchMgr}
}

func (tm *tradeManager) OnCandleUpdate(symbol string) {
	if tm == nil || !tm.cfg.autoTrade {
		return
	}
	symbol = strings.ToUpper(symbol)
	if symbol == "" {
		return
	}
	go tm.handleCandleUpdate(symbol)
}

func (tm *tradeManager) handleCandleUpdate(symbol string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := tm.checkPositionForSymbol(ctx, symbol); err != nil {
		log.Printf("基于3m更新检查持仓失败 %s: %v", symbol, err)
	}
	if err := tm.evaluateOpenForSymbol(ctx, symbol); err != nil {
		log.Printf("基于3m更新评估开仓失败 %s: %v", symbol, err)
	}
}

func (tm *tradeManager) checkPositionForSymbol(ctx context.Context, symbol string) error {
	symbol = strings.ToUpper(symbol)
	if symbol == "" || tm.watchMgr == nil {
		return nil
	}
	hasLong := tm.positions.Has(symbol, "LONG")
	hasShort := tm.positions.Has(symbol, "SHORT")
	if !hasLong && !hasShort {
		return nil
	}
	if err := tm.refreshPositions(ctx); err != nil {
		return err
	}
	hasLong = tm.positions.Has(symbol, "LONG")
	hasShort = tm.positions.Has(symbol, "SHORT")
	if !hasLong && !hasShort {
		return nil
	}
	sr, err := computeStrategyForSymbol(ctx, tm.watchMgr, symbol, tm.cfg)
	if err != nil {
		return err
	}
	var firstErr error
	if hasLong && shouldClosePosition("LONG", sr) {
		if err := tm.closePosition(ctx, symbol, "LONG"); err != nil {
			log.Printf("3m 平多失败 %s: %v", symbol, err)
			tm.notifyError(ctx, fmt.Sprintf("平多失败 %s: %v", symbol, err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if hasShort && shouldClosePosition("SHORT", sr) {
		if err := tm.closePosition(ctx, symbol, "SHORT"); err != nil {
			log.Printf("3m 平空失败 %s: %v", symbol, err)
			tm.notifyError(ctx, fmt.Sprintf("平空失败 %s: %v", symbol, err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (tm *tradeManager) evaluateOpenForSymbol(ctx context.Context, symbol string) error {
	if tm == nil || tm.watchMgr == nil {
		return nil
	}
	if tm.positions.Remaining() <= 0 {
		return nil
	}
	sr, err := computeStrategyForSymbol(ctx, tm.watchMgr, symbol, tm.cfg)
	if err != nil {
		return err
	}
	if sr.Score <= 0 {
		return nil
	}
	price := sr.Metrics.Last
	if price <= 0 {
		return nil
	}
	var firstErr error
	if sr.LongSignal && !tm.positions.Has(symbol, "LONG") {
		if err := tm.openPosition(ctx, symbol, "LONG", price); err != nil {
			log.Printf("实时开多失败 %s: %v", symbol, err)
			tm.notifyError(ctx, fmt.Sprintf("开多失败 %s: %v", symbol, err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if tm.positions.Remaining() <= 0 {
		return firstErr
	}
	if sr.ShortSignal && !tm.positions.Has(symbol, "SHORT") {
		if err := tm.openPosition(ctx, symbol, "SHORT", price); err != nil {
			log.Printf("实时开空失败 %s: %v", symbol, err)
			tm.notifyError(ctx, fmt.Sprintf("开空失败 %s: %v", symbol, err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func shouldClosePosition(side string, sr strategyResult) bool {
	tf := sr.ThreeMinute
	switch strings.ToUpper(side) {
	case "LONG":
		return tf.EMAFast <= tf.EMASlow || tf.MACDHist <= 0
	case "SHORT":
		return tf.EMAFast >= tf.EMASlow || tf.MACDHist >= 0
	default:
		return false
	}
}

func computeStrategyForSymbol(ctx context.Context, watchMgr *symbolManager, symbol string, cfg config) (strategyResult, error) {
	symbol = strings.ToUpper(symbol)
	if symbol == "" {
		return strategyResult{}, errors.New("symbol 为空")
	}
	if watchMgr == nil {
		return strategyResult{}, errors.New("行情管理器未初始化")
	}
	w := watchMgr.getWatcher(symbol)
	if w == nil {
		watchMgr.EnsureWatchers(ctx, []string{symbol})
		w = watchMgr.getWatcher(symbol)
		if w == nil {
			return strategyResult{}, fmt.Errorf("未找到 %s 的行情缓存", symbol)
		}
	}
	candles := w.snapshot()
	if len(candles) == 0 {
		return strategyResult{}, errors.New("行情数据不足")
	}
	return computeStrategyFromCandles(symbol, candles, cfg)
}

func (tm *tradeManager) refreshPositions(ctx context.Context) error {
	entries, err := tm.client.FetchOpenPositions(ctx)
	if err != nil {
		return err
	}
	tm.positions.Replace(entries)
	return nil
}

func (tm *tradeManager) CheckPositionExits(ctx context.Context) error {
	if tm == nil {
		return errors.New("trade manager 未初始化")
	}
	if err := tm.refreshPositions(ctx); err != nil {
		return err
	}
	entries := tm.positions.Entries()
	if len(entries) == 0 {
		return nil
	}

	log.Printf("开始持仓检查，当前持仓数: %d", len(entries))
	if tm.watchMgr != nil {
		symbolSet := make(map[string]struct{}, len(entries))
		for _, entry := range entries {
			symbol := strings.ToUpper(entry.Symbol)
			if symbol == "" {
				continue
			}
			symbolSet[symbol] = struct{}{}
		}
		if len(symbolSet) > 0 {
			symbols := make([]string, 0, len(symbolSet))
			for sym := range symbolSet {
				symbols = append(symbols, sym)
			}
			tm.watchMgr.EnsureWatchers(ctx, symbols)
		}
	}

	strategies := make(map[string]strategyResult, len(entries))
	for _, entry := range entries {
		symbol := strings.ToUpper(entry.Symbol)
		if symbol == "" {
			continue
		}
		if _, exists := strategies[symbol]; exists {
			continue
		}
		sr, err := computeStrategyForSymbol(ctx, tm.watchMgr, symbol, tm.cfg)
		if err != nil {
			log.Printf("获取 %s 策略数据失败: %v", symbol, err)
			continue
		}
		strategies[symbol] = sr
	}

	var firstErr error
	for _, entry := range entries {
		symbol := strings.ToUpper(entry.Symbol)
		side := strings.ToUpper(entry.Side)
		if symbol == "" || side == "" {
			continue
		}
		sr, ok := strategies[symbol]
		if !ok {
			continue
		}
		if !shouldClosePosition(side, sr) {
			continue
		}
		err := tm.closePosition(ctx, symbol, side)
		if err != nil {
			log.Printf("5m 检查平仓失败 %s %s: %v", symbol, side, err)
			tm.notifyError(ctx, fmt.Sprintf("平仓失败 %s %s: %v", symbol, side, err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

func (tm *tradeManager) HandleSignals(ctx context.Context, strategies []strategyResult) {
	if err := tm.refreshPositions(ctx); err != nil {
		log.Printf("刷新持仓失败: %v", err)
	}
	if len(strategies) > 1 {
		sort.Slice(strategies, func(i, j int) bool {
			return strategies[i].Score > strategies[j].Score
		})
	}
	if len(strategies) > 0 {
		metrics := make([]symbolMetrics, 0, len(strategies))
		for _, sr := range strategies {
			metrics = append(metrics, sr.Metrics)
		}
		tm.UpdateQuantitiesFromMetrics(ctx, metrics)
	}

	for _, sr := range strategies {
		if sr.Score <= 0 {
			continue
		}
		if tm.positions.Remaining() <= 0 {
			break
		}
		price := sr.Metrics.Last
		if price <= 0 {
			continue
		}
		if sr.LongSignal && !tm.positions.Has(sr.Symbol, "LONG") {
			if err := tm.openPosition(ctx, sr.Symbol, "LONG", price); err != nil {
				log.Printf("开多单失败 %s: %v", sr.Symbol, err)
				tm.notifyError(ctx, fmt.Sprintf("开多失败 %s: %v", sr.Symbol, err))
			}
		}
		if tm.positions.Remaining() <= 0 {
			break
		}
		if sr.ShortSignal && !tm.positions.Has(sr.Symbol, "SHORT") {
			if err := tm.openPosition(ctx, sr.Symbol, "SHORT", price); err != nil {
				log.Printf("开空单失败 %s: %v", sr.Symbol, err)
				tm.notifyError(ctx, fmt.Sprintf("开空失败 %s: %v", sr.Symbol, err))
			}
		}
	}
}

func (tm *tradeManager) openPosition(ctx context.Context, symbol, side string, price float64) error {
	if tm.positions.Count() >= tm.cfg.maxPositions {
		return fmt.Errorf("已达到最大持仓数量")
	}
	qty, err := tm.determineOpenQuantity(ctx, symbol, price)
	if err != nil {
		return err
	}
	if err := tm.client.EnsureSymbolSettings(ctx, symbol, tm.cfg.leverage); err != nil {
		return err
	}
	order := orderRequest{
		Symbol:       symbol,
		Quantity:     qty,
		PositionSide: side,
	}
	switch side {
	case "LONG":
		order.Side = "BUY"
	case "SHORT":
		order.Side = "SELL"
	default:
		return fmt.Errorf("未知的持仓方向: %s", side)
	}
	log.Printf("下单请求: %s %s 数量 %s", symbol, side, formatQuantity(qty))
	if err := tm.client.PlaceOrder(ctx, order); err != nil {
		return err
	}
	if err := tm.refreshPositions(ctx); err != nil {
		log.Printf("刷新持仓失败: %v", err)
		tm.positions.Set(symbol, side, qty)
	}
	log.Printf("%s %s 开仓成功，数量 %s", symbol, side, formatQuantity(qty))
	return nil
}

func (tm *tradeManager) determineOpenQuantity(ctx context.Context, symbol string, price float64) (float64, error) {
	if price <= 0 {
		return 0, fmt.Errorf("%s 无效价格", symbol)
	}
	if tm.cfg.orderQty > 0 {
		return tm.client.AdjustQuantity(ctx, symbol, tm.cfg.orderQty)
	}
	walletBalance, err := tm.client.FetchWalletBalance(ctx)
	if err != nil {
		return 0, err
	}
	if walletBalance <= 0 {
		return 0, fmt.Errorf("账户余额不足")
	}
	margin := walletBalance / 10.0
	if margin <= 0 {
		return 0, fmt.Errorf("无有效名义资金")
	}
	notional := margin * float64(tm.cfg.leverage)
	if notional <= 0 {
		return 0, fmt.Errorf("无有效名义资金")
	}
	qty := notional / price
	return tm.client.AdjustQuantity(ctx, symbol, qty)
}

func (tm *tradeManager) notifyError(ctx context.Context, msg string) {
	if tm.httpClient == nil || msg == "" {
		return
	}
	if tm.cfg.telegramToken == "" || tm.cfg.telegramChatID == "" {
		return
	}
	text := "交易错误: " + msg
	if err := sendTelegramMessages(ctx, tm.httpClient, tm.cfg.telegramToken, tm.cfg.telegramChatID, text); err != nil {
		log.Printf("发送交易错误提醒失败: %v", err)
	}
}

func (tm *tradeManager) UpdateQuantitiesFromMetrics(ctx context.Context, metrics []symbolMetrics) map[string]float64 {
	tm.qtyMu.Lock()
	tm.qty = make(map[string]float64)
	tm.qtyMu.Unlock()
	if len(metrics) == 0 {
		return nil
	}
	remaining := tm.positions.Remaining()
	if remaining <= 0 {
		return tm.snapshotQuantities()
	}
	var walletBalance float64
	var availErr error
	if tm.cfg.orderQty <= 0 {
		walletBalance, availErr = tm.client.FetchWalletBalance(ctx)
		if availErr != nil {
			log.Printf("获取账户余额失败: %v", availErr)
			return tm.snapshotQuantities()
		}
		if walletBalance <= 0 {
			log.Printf("账户余额不足，无法计算下单数量")
			return tm.snapshotQuantities()
		}
	}
	margin := 0.0
	if tm.cfg.orderQty <= 0 {
		margin = walletBalance / 10.0
		if margin <= 0 {
			log.Printf("账户余额不足，无法根据 1/10 规则计算下单数量")
			return tm.snapshotQuantities()
		}
	}
	for _, m := range metrics {
		price := m.Last
		if price <= 0 {
			continue
		}
		var qty float64
		var err error
		if tm.cfg.orderQty > 0 {
			qty, err = tm.client.AdjustQuantity(ctx, m.Symbol, tm.cfg.orderQty)
		} else {
			notional := margin * float64(tm.cfg.leverage)
			if notional <= 0 {
				continue
			}
			quantity := notional / price
			qty, err = tm.client.AdjustQuantity(ctx, m.Symbol, quantity)
		}
		if err != nil || qty <= 0 {
			continue
		}
		tm.setCachedQty(m.Symbol, qty)
	}
	return tm.snapshotQuantities()
}

func (tm *tradeManager) setCachedQty(symbol string, qty float64) {
	tm.qtyMu.Lock()
	defer tm.qtyMu.Unlock()
	if tm.qty == nil {
		tm.qty = make(map[string]float64)
	}
	tm.qty[strings.ToUpper(symbol)] = qty
}

func (tm *tradeManager) snapshotQuantities() map[string]float64 {
	tm.qtyMu.RLock()
	defer tm.qtyMu.RUnlock()
	if len(tm.qty) == 0 {
		return nil
	}
	out := make(map[string]float64, len(tm.qty))
	for k, v := range tm.qty {
		out[k] = v
	}
	return out
}

func (tm *tradeManager) closePosition(ctx context.Context, symbol, side string) error {
	entry, ok := tm.positions.Get(symbol, side)
	if !ok {
		return nil
	}
	qty := entry.Qty
	if qty <= 0 {
		qty = tm.cfg.orderQty
	}
	adjQty, err := tm.client.AdjustQuantity(ctx, symbol, qty)
	if err != nil {
		return err
	}
	order := orderRequest{
		Symbol:       symbol,
		Quantity:     adjQty,
		PositionSide: side,
	}
	switch side {
	case "LONG":
		order.Side = "SELL"
	case "SHORT":
		order.Side = "BUY"
	default:
		return fmt.Errorf("未知的持仓方向: %s", side)
	}
	log.Printf("平仓请求: %s %s 数量 %s", symbol, side, formatQuantity(adjQty))
	if err := tm.client.PlaceOrder(ctx, order); err != nil {
		return err
	}
	if err := tm.refreshPositions(ctx); err != nil {
		log.Printf("刷新持仓失败: %v", err)
		tm.positions.Remove(symbol, side)
	}
	log.Printf("%s %s 平仓成功，数量 %s", symbol, side, formatQuantity(adjQty))
	return nil
}

func fetchHistorical1m(ctx context.Context, c *http.Client, symbol string, limit int) ([]candle, error) {
	url := fmt.Sprintf("%s%s?symbol=%s&interval=1m&limit=%d", baseURL, klinesEP, symbol, limit)
	var raw [][]interface{}
	if err := getJSON(ctx, c, url, &raw); err != nil {
		return nil, err
	}
	candles := make([]candle, 0, len(raw))
	for _, row := range raw {
		if len(row) < 6 {
			continue
		}
		opTimeFloat, ok := row[0].(float64)
		if !ok {
			continue
		}
		op := parseStringFloat(row[1])
		high := parseStringFloat(row[2])
		low := parseStringFloat(row[3])
		cl := parseStringFloat(row[4])
		vol := parseStringFloat(row[5])
		if math.IsNaN(op) || math.IsNaN(high) || math.IsNaN(low) || math.IsNaN(cl) {
			continue
		}
		openTime := time.UnixMilli(int64(opTimeFloat))
		candles = append(candles, candle{
			OpenTime:  openTime,
			CloseTime: openTime.Add(time.Minute),
			Open:      op,
			High:      high,
			Low:       low,
			Close:     cl,
			Volume:    vol,
		})
	}
	if len(candles) == 0 {
		return nil, errors.New("无历史K线数据")
	}
	return dedupeCandles(candles), nil
}

func parseStringFloat(v interface{}) float64 {
	switch val := v.(type) {
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return math.NaN()
		}
		return f
	case float64:
		return val
	default:
		return math.NaN()
	}
}

type config struct {
	concurrency      int
	top              int
	updateInterval   time.Duration
	volumeRefresh    time.Duration
	telegramToken    string
	telegramChatID   string
	watchCount       int
	emaFastPeriod    int
	emaSlowPeriod    int
	macdFastPeriod   int
	macdSlowPeriod   int
	macdSignalPeriod int
	adxPeriod        int
	adxThreshold     float64
	autoTrade        bool
	orderQty         float64
	maxPositions     int
	leverage         int
	recvWindow       int
	apiKey           string
	apiSecret        string
}

func main() {
	if err := loadEnvFile(".env"); err != nil {
		fmt.Fprintf(os.Stderr, "加载 .env 失败: %v\n", err)
	}

	cfg := parseConfig()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
		os.Exit(1)
	}
}

func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		idx := strings.IndexRune(line, '=')
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, val)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func parseConfig() config {
	concurrency := flag.Int("concurrency", 10, "并发请求数量")
	top := flag.Int("top", 20, "输出涨跌幅排名数量")
	updateInterval := flag.Duration("update-interval", 10*time.Minute, "涨跌幅更新周期")
	volumeRefresh := flag.Duration("volume-refresh", 12*time.Hour, "成交量榜刷新周期")
	watchCount := flag.Int("watch-count", 20, "监听币种数量")
	emaFast := flag.Int("ema-fast", 7, "5m EMA 快速周期")
	emaSlow := flag.Int("ema-slow", 25, "5m EMA 慢速周期")
	macdFast := flag.Int("macd-fast", 12, "MACD 快速 EMA 周期")
	macdSlow := flag.Int("macd-slow", 26, "MACD 慢速 EMA 周期")
	macdSignal := flag.Int("macd-signal", 9, "MACD 信号线 EMA 周期")
	adxPeriod := flag.Int("adx-period", 14, "5m ADX 周期")
	adxThreshold := flag.Float64("adx-threshold", 25, "ADX 趋势判断阈值")
	autoTrade := flag.Bool("auto-trade", true, "启用自动下单")
	orderQty := flag.Float64("order-qty", 0, "每次下单的合约张数/数量")
	maxPositions := flag.Int("max-positions", 10, "最大持仓数量")
	leverage := flag.Int("leverage", 5, "杠杆倍数")
	recvWindow := flag.Int("recv-window", 5000, "Binance API recvWindow (毫秒)")
	flag.Parse()

	qty := *orderQty
	if qty <= 0 {
		if v := os.Getenv("BINANCE_ORDER_QTY"); v != "" {
			if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed > 0 {
				qty = parsed
			}
		}
	}

	cfg := config{
		concurrency:      *concurrency,
		top:              *top,
		updateInterval:   *updateInterval,
		volumeRefresh:    *volumeRefresh,
		telegramToken:    os.Getenv("TELEGRAM_BOT_TOKEN"),
		telegramChatID:   os.Getenv("TELEGRAM_CHAT_ID"),
		watchCount:       *watchCount,
		emaFastPeriod:    *emaFast,
		emaSlowPeriod:    *emaSlow,
		macdFastPeriod:   *macdFast,
		macdSlowPeriod:   *macdSlow,
		macdSignalPeriod: *macdSignal,
		adxPeriod:        *adxPeriod,
		adxThreshold:     *adxThreshold,
		autoTrade:        *autoTrade,
		orderQty:         qty,
		maxPositions:     *maxPositions,
		leverage:         *leverage,
		recvWindow:       *recvWindow,
		apiKey:           os.Getenv("BINANCE_API_KEY"),
		apiSecret:        os.Getenv("BINANCE_API_SECRET"),
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	if cfg.top < 1 {
		cfg.top = 1
	}
	if cfg.updateInterval <= 0 {
		cfg.updateInterval = 10 * time.Minute
	}
	if cfg.volumeRefresh <= 0 {
		cfg.volumeRefresh = 12 * time.Hour
	}
	if cfg.watchCount < 1 {
		cfg.watchCount = cfg.top
	}
	if cfg.watchCount < cfg.top {
		cfg.watchCount = cfg.top
	}
	if cfg.emaFastPeriod < 1 {
		cfg.emaFastPeriod = 7
	}
	if cfg.emaSlowPeriod <= cfg.emaFastPeriod {
		cfg.emaSlowPeriod = cfg.emaFastPeriod + 18
	}
	if cfg.macdFastPeriod < 1 {
		cfg.macdFastPeriod = 12
	}
	if cfg.macdSlowPeriod <= cfg.macdFastPeriod {
		cfg.macdSlowPeriod = cfg.macdFastPeriod + 14
	}
	if cfg.macdSignalPeriod < 1 {
		cfg.macdSignalPeriod = 9
	}
	if cfg.adxPeriod < 1 {
		cfg.adxPeriod = 14
	}
	if cfg.adxThreshold <= 0 {
		cfg.adxThreshold = 25
	}
	if cfg.maxPositions < 1 {
		cfg.maxPositions = 10
	}
	if cfg.leverage < 1 {
		cfg.leverage = 5
	}
	if cfg.recvWindow <= 0 {
		cfg.recvWindow = 5000
	}
	return cfg
}

func initializeExistingPositions(ctx context.Context, tradeMgr *tradeManager) {
	if ctx == nil || tradeMgr == nil {
		return
	}
	entries := tradeMgr.positions.Entries()
	if len(entries) == 0 {
		return
	}
	log.Printf("启动时检测持仓，待检查 %d 个仓位", len(entries))
	if tradeMgr.watchMgr != nil {
		symbolSet := make(map[string]struct{}, len(entries))
		for _, entry := range entries {
			symbol := strings.ToUpper(entry.Symbol)
			if symbol == "" {
				continue
			}
			symbolSet[symbol] = struct{}{}
		}
		if len(symbolSet) > 0 {
			symbols := make([]string, 0, len(symbolSet))
			for sym := range symbolSet {
				symbols = append(symbols, sym)
			}
			tradeMgr.watchMgr.EnsureWatchers(ctx, symbols)
		}
	}
	if err := tradeMgr.CheckPositionExits(ctx); err != nil {
		log.Printf("启动时持仓检查出现错误: %v", err)
	}
}

func run(cfg config) error {
	if cfg.telegramToken == "" || cfg.telegramChatID == "" {
		return errors.New("未配置 Telegram 凭证：请在环境变量 TELEGRAM_BOT_TOKEN、TELEGRAM_CHAT_ID 中设置")
	}
	if cfg.autoTrade {
		if cfg.apiKey == "" || cfg.apiSecret == "" {
			return errors.New("启用自动下单需要配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	httpClient := &http.Client{Timeout: 12 * time.Second}
	watchMgr := newSymbolManager(httpClient, cfg)
	defer watchMgr.Close()

	var tradeMgr *tradeManager
	if cfg.autoTrade {
		binance := newBinanceClient(httpClient, cfg)
		if err := binance.EnsureDualSide(ctx); err != nil {
			return fmt.Errorf("设置双向持仓失败: %w", err)
		}
		positions, err := binance.FetchOpenPositions(ctx)
		if err != nil {
			return fmt.Errorf("获取当前持仓失败: %w", err)
		}
		tradeMgr = newTradeManager(binance, positions, cfg, httpClient, watchMgr)
		log.Printf("当前持仓数量: %d / %d", tradeMgr.positions.Count(), cfg.maxPositions)
		watchMgr.SetCandleHandler(tradeMgr.OnCandleUpdate)
		initializeExistingPositions(ctx, tradeMgr)
	}

	var (
		symbols   []string
		symbolsMu sync.RWMutex
	)

	refreshVolumeList := func(parent context.Context) error {
		ctxRefresh, cancel := context.WithTimeout(parent, 2*time.Minute)
		defer cancel()

		allowed, err := fetchUSDTPerps(ctxRefresh, httpClient)
		if err != nil {
			return fmt.Errorf("获取U本位合约失败: %w", err)
		}
		if len(allowed) == 0 {
			return errors.New("未获取到任何U本位永续合约")
		}

		all, err := fetchAllTickers(ctxRefresh, httpClient)
		if err != nil {
			return fmt.Errorf("获取24h ticker失败: %w", err)
		}
		filt := filterAndRankTopHalfByVolume(all, allowed)
		if len(filt) == 0 {
			return errors.New("筛选后无可用交易对")
		}

		symbolsMu.Lock()
		symbols = filt
		symbolsMu.Unlock()
		log.Printf("已刷新成交量前50%%交易对，共 %d 个", len(filt))
		return nil
	}

	computeAndNotify := func(parent context.Context) error {
		symbolsMu.RLock()
		snapshot := append([]string(nil), symbols...)
		symbolsMu.RUnlock()
		if len(snapshot) == 0 {
			return errors.New("当前成交量列表为空，等待下一次刷新")
		}

		ctxUpdate, cancel := context.WithTimeout(parent, 4*time.Minute)
		defer cancel()

		results := computeSymbolMetrics(ctxUpdate, httpClient, snapshot, cfg.concurrency)
		if len(results) == 0 {
			return errors.New("未能计算任何交易对的10分钟涨跌幅")
		}

		gainers, losers := splitTopMovers(results, cfg.top)
		watchSet := make(map[string]struct{}, len(snapshot))
		for _, sym := range snapshot {
			upper := strings.ToUpper(sym)
			if upper == "" {
				continue
			}
			watchSet[upper] = struct{}{}
		}
		if cfg.autoTrade && tradeMgr != nil {
			for _, sym := range tradeMgr.positions.Symbols() {
				upper := strings.ToUpper(sym)
				if upper == "" {
					continue
				}
				watchSet[upper] = struct{}{}
			}
		}
		watchSymbols := make([]string, 0, len(watchSet))
		for sym := range watchSet {
			watchSymbols = append(watchSymbols, sym)
		}
		sort.Strings(watchSymbols)
		if tradeMgr != nil && tradeMgr.watchMgr != nil {
			tradeMgr.watchMgr.EnsureWatchers(ctx, watchSymbols)
		} else if len(watchSymbols) > 0 {
			watchMgr.EnsureWatchers(ctx, watchSymbols)
		}
		strategies := watchMgr.EvaluateStrategies(watchSymbols)
		var qtyMap map[string]float64
		if cfg.autoTrade && tradeMgr != nil {
			qtyMap = tradeMgr.UpdateQuantitiesFromMetrics(ctxUpdate, results)
		}
		if cfg.autoTrade && tradeMgr != nil {
			tradeMgr.HandleSignals(ctxUpdate, strategies)
			qtyMap = tradeMgr.snapshotQuantities()
		}
		message := buildTelegramMessage(time.Now(), len(snapshot), cfg.top, gainers, losers, strategies, watchSymbols, qtyMap)
		if message == "" {
			return errors.New("推送内容为空")
		}

		if err := sendTelegramMessages(ctxUpdate, httpClient, cfg.telegramToken, cfg.telegramChatID, message); err != nil {
			return fmt.Errorf("发送Telegram消息失败: %w", err)
		}
		if alerts := buildSignalAlerts(strategies, qtyMap); len(alerts) > 0 {
			if err := sendTelegramMessages(ctxUpdate, httpClient, cfg.telegramToken, cfg.telegramChatID, alerts); err != nil {
				log.Printf("发送信号提醒失败: %v", err)
			}
		}
		log.Printf("已推送Telegram通知，涨幅榜: %d 条，跌幅榜: %d 条，监听币种: %d", len(gainers), len(losers), len(watchSymbols))
		return nil
	}

	positionStatus := func(parent context.Context) error {
		if tradeMgr == nil {
			return nil
		}
		ctxStatus, cancel := context.WithTimeout(parent, 90*time.Second)
		defer cancel()

		if err := tradeMgr.refreshPositions(ctxStatus); err != nil {
			log.Printf("刷新持仓失败: %v", err)
		}
		entries := tradeMgr.positions.Entries()
		symbolSet := make(map[string]struct{})
		symbols := make([]string, 0, len(entries))
		for _, entry := range entries {
			symbol := strings.ToUpper(entry.Symbol)
			if symbol == "" {
				continue
			}
			if _, exists := symbolSet[symbol]; exists {
				continue
			}
			symbolSet[symbol] = struct{}{}
			symbols = append(symbols, symbol)
		}
		strategyMap := make(map[string]strategyResult, len(symbols))
		if len(symbols) > 0 {
			if tradeMgr.watchMgr != nil {
				tradeMgr.watchMgr.EnsureWatchers(parent, symbols)
			}
			for _, sym := range symbols {
				sr, err := computeStrategyForSymbol(ctxStatus, tradeMgr.watchMgr, sym, cfg)
				if err != nil {
					log.Printf("获取持仓 %s 策略数据失败: %v", sym, err)
					continue
				}
				strategyMap[strings.ToUpper(sr.Symbol)] = sr
			}
		}
		message := buildPositionStatusMessage(time.Now(), entries, strategyMap)
		if message == "" {
			return nil
		}
		if err := sendTelegramMessages(ctxStatus, httpClient, cfg.telegramToken, cfg.telegramChatID, message); err != nil {
			return fmt.Errorf("发送持仓状态失败: %w", err)
		}
		log.Printf("已推送持仓状态，持仓数: %d", len(entries))
		return nil
	}

	if err := refreshVolumeList(ctx); err != nil {
		return err
	}

	if err := computeAndNotify(ctx); err != nil {
		log.Printf("首次推送失败: %v", err)
	}

	volumeTicker := time.NewTicker(cfg.volumeRefresh)
	defer volumeTicker.Stop()
	updateTicker := time.NewTicker(cfg.updateInterval)
	defer updateTicker.Stop()
	positionTicker := time.NewTicker(6 * time.Minute)
	defer positionTicker.Stop()

	log.Printf("启动完成：成交量刷新周期 %s，涨跌幅推送周期 %s", cfg.volumeRefresh, cfg.updateInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("收到终止信号，程序退出")
			return nil
		case <-volumeTicker.C:
			if err := refreshVolumeList(ctx); err != nil {
				log.Printf("刷新成交量列表失败: %v", err)
			}
		case <-updateTicker.C:
			if err := computeAndNotify(ctx); err != nil {
				log.Printf("推送Telegram失败: %v", err)
			}
		case <-positionTicker.C:
			if err := positionStatus(ctx); err != nil {
				log.Printf("推送持仓状态失败: %v", err)
			}
		}
	}
}

func fetchUSDTPerps(ctx context.Context, c *http.Client) (map[string]struct{}, error) {
	url := baseURL + exchangeInfoEP
	var resp exchangeInfoResp
	if err := getJSON(ctx, c, url, &resp); err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{})
	for _, s := range resp.Symbols {
		if s.Status == "TRADING" && s.QuoteAsset == "USDT" && s.ContractType == "PERPETUAL" {
			allowed[s.Symbol] = struct{}{}
		}
	}
	return allowed, nil
}

func fetchAllTickers(ctx context.Context, c *http.Client) ([]ticker24hr, error) {
	url := baseURL + ticker24hrEP
	var out []ticker24hr
	if err := getJSON(ctx, c, url, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func filterAndRankTopHalfByVolume(all []ticker24hr, allowed map[string]struct{}) []string {
	type pair struct {
		sym string
		vol float64
	}
	pairs := make([]pair, 0, len(all))
	for _, t := range all {
		if _, ok := allowed[t.Symbol]; !ok {
			continue
		}
		vol, err := strconv.ParseFloat(t.Volume, 64)
		if err != nil {
			continue
		}
		pairs = append(pairs, pair{sym: t.Symbol, vol: vol})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].vol > pairs[j].vol })
	if len(pairs) == 0 {
		return nil
	}
	half := int(math.Ceil(float64(len(pairs)) / 2.0))
	top := make([]string, 0, half)
	for i := 0; i < half; i++ {
		top = append(top, pairs[i].sym)
	}
	return top
}

func computeSymbolMetrics(ctx context.Context, c *http.Client, symbols []string, concurrency int) []symbolMetrics {
	if concurrency < 1 {
		concurrency = 1
	}
	// 控制并发，尽量温和调用，避免触发限频
	sem := make(chan struct{}, concurrency)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	res := make([]symbolMetrics, 0, len(symbols))

	for _, s := range symbols {
		s := s
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			metrics, err := fetchSymbolMetrics(ctx, c, s)
			if err != nil {
				return
			}
			mu.Lock()
			res = append(res, metrics)
			mu.Unlock()
		}()
	}
	wg.Wait()

	// 按绝对涨跌幅降序（基于 30 分钟涨跌幅）
	sort.Slice(res, func(i, j int) bool {
		ai := math.Abs(res[i].Change30m)
		aj := math.Abs(res[j].Change30m)
		if ai == aj {
			return res[i].Change30m > res[j].Change30m
		}
		return ai > aj
	})
	return res
}

func splitTopMovers(results []symbolMetrics, top int) (gainers []symbolMetrics, losers []symbolMetrics) {
	if top < 1 {
		top = 1
	}
	for _, r := range results {
		if r.Change30m > 0 {
			gainers = append(gainers, r)
		} else if r.Change30m < 0 {
			losers = append(losers, r)
		}
	}
	sort.Slice(gainers, func(i, j int) bool { return gainers[i].Change30m > gainers[j].Change30m })
	sort.Slice(losers, func(i, j int) bool { return losers[i].Change30m < losers[j].Change30m })
	if len(gainers) > top {
		gainers = gainers[:top]
	}
	if len(losers) > top {
		losers = losers[:top]
	}
	return gainers, losers
}

func selectTopSymbols(results []symbolMetrics, count int) []string {
	if count <= 0 {
		return nil
	}
	if len(results) < count {
		count = len(results)
	}
	symbols := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for _, r := range results {
		sym := strings.ToUpper(r.Symbol)
		if sym == "" {
			continue
		}
		if _, ok := seen[sym]; ok {
			continue
		}
		symbols = append(symbols, sym)
		seen[sym] = struct{}{}
		if len(symbols) >= count {
			break
		}
	}
	return symbols
}

func buildTelegramMessage(now time.Time, volumeCount int, top int, gainers, losers []symbolMetrics, strategies []strategyResult, watchSymbols []string, qtyMap map[string]float64) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("时间: %s\n", now.Format("2006-01-02 15:04:05")))
	b.WriteString(fmt.Sprintf("成交量前50%%交易对数: %d\n", volumeCount))
	if len(watchSymbols) > 0 {
		b.WriteString(fmt.Sprintf("当前监听币种(%d): %s\n", len(watchSymbols), strings.Join(watchSymbols, ", ")))
	}
	b.WriteString("信号列表 (仅展示满足开仓条件的标的)：\n")
	printed := false
	for _, sr := range strategies {
		if sr.LongSignal {
			printed = true
			b.WriteString(fmt.Sprintf("[开多] %-10s Score:%0.2f 30m:%+0.4f%% 10m:%+0.4f%% Avg振幅:%0.4f%% | %s | %s%s\n",
				sr.Symbol,
				sr.Score,
				sr.Metrics.Change30m,
				sr.Metrics.Change10m,
				sr.Metrics.AvgAmplitude,
				summarizeTimeframe(sr.FiveMinute),
				summarizeTimeframe(sr.ThreeMinute),
				formatQtyInfo(qtyMap, sr.Symbol),
			))
		}
		if sr.ShortSignal {
			printed = true
			b.WriteString(fmt.Sprintf("[开空] %-10s Score:%0.2f 30m:%+0.4f%% 10m:%+0.4f%% Avg振幅:%0.4f%% | %s | %s%s\n",
				sr.Symbol,
				sr.Score,
				sr.Metrics.Change30m,
				sr.Metrics.Change10m,
				sr.Metrics.AvgAmplitude,
				summarizeTimeframe(sr.FiveMinute),
				summarizeTimeframe(sr.ThreeMinute),
				formatQtyInfo(qtyMap, sr.Symbol),
			))
		}
	}
	if !printed {
		b.WriteString("暂无可开仓信号\n")
	}

	b.WriteString(fmt.Sprintf("\n涨幅榜 (Top %d)：\n", top))
	for i, g := range gainers {
		if i >= top {
			break
		}
		b.WriteString(fmt.Sprintf("%2d) %-12s 30m:%+0.4f%% 10m:%+0.4f%% 收盘价: %.8f\n", i+1, g.Symbol, g.Change30m, g.Change10m, g.Last))
	}

	b.WriteString(fmt.Sprintf("\n跌幅榜 (Top %d)：\n", top))
	for i, l := range losers {
		if i >= top {
			break
		}
		b.WriteString(fmt.Sprintf("%2d) %-12s 30m:%+0.4f%% 10m:%+0.4f%% 收盘价: %.8f\n", i+1, l.Symbol, l.Change30m, l.Change10m, l.Last))
	}

	return strings.TrimSpace(b.String())
}

func formatQtyInfo(qtyMap map[string]float64, symbol string) string {
	if qtyMap == nil {
		return ""
	}
	q, ok := qtyMap[strings.ToUpper(symbol)]
	if !ok || q <= 0 {
		return ""
	}
	return fmt.Sprintf(" Qty:%s", formatQuantity(q))
}

func summarizeTimeframe(tf timeframeAnalysis) string {
	return fmt.Sprintf("%s EMA快:%0.4f(Δ%+0.4f) EMA慢:%0.4f(Δ%+0.4f) MACD_H:%+0.4f→%+0.4f ADX:%0.2f",
		tf.Interval,
		tf.EMAFast,
		tf.EMAFastSlope,
		tf.EMASlow,
		tf.EMASlowSlope,
		tf.MACDHist,
		tf.MACDPrev,
		tf.ADX,
	)
}

func buildSignalAlerts(strategies []strategyResult, qtyMap map[string]float64) string {
	if len(strategies) == 0 {
		return ""
	}
	var lines []string
	for _, sr := range strategies {
		if !sr.LongSignal && !sr.ShortSignal {
			continue
		}
		qtyInfo := formatQtyInfo(qtyMap, sr.Symbol)
		if sr.LongSignal {
			lines = append(lines, fmt.Sprintf("[开多] %s%s Score:%0.2f 30m:%+.4f%% 10m:%+.4f%% 5m MACD_H:%+.4f ADX:%.2f | 3m MACD_H:%+.4f ADX:%.2f",
				sr.Symbol,
				qtyInfo,
				sr.Score,
				sr.Metrics.Change30m,
				sr.Metrics.Change10m,
				sr.FiveMinute.MACDHist,
				sr.FiveMinute.ADX,
				sr.ThreeMinute.MACDHist,
				sr.ThreeMinute.ADX,
			))
		}
		if sr.ShortSignal {
			lines = append(lines, fmt.Sprintf("[开空] %s%s Score:%0.2f 30m:%+.4f%% 10m:%+.4f%% 5m MACD_H:%+.4f ADX:%.2f | 3m MACD_H:%+.4f ADX:%.2f",
				sr.Symbol,
				qtyInfo,
				sr.Score,
				sr.Metrics.Change30m,
				sr.Metrics.Change10m,
				sr.FiveMinute.MACDHist,
				sr.FiveMinute.ADX,
				sr.ThreeMinute.MACDHist,
				sr.ThreeMinute.ADX,
			))
		}
	}
	if len(lines) == 0 {
		return ""
	}
	return "信号提醒:\n" + strings.Join(lines, "\n")
}

func buildPositionStatusMessage(now time.Time, entries []positionEntry, srMap map[string]strategyResult) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("持仓监控（6m）\n时间: %s\n", now.Format("2006-01-02 15:04:05")))
	if len(entries) == 0 {
		b.WriteString("当前无持仓")
		return strings.TrimSpace(b.String())
	}
	b.WriteString(fmt.Sprintf("当前持仓数: %d\n", len(entries)))

	sorted := append([]positionEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		si := strings.ToUpper(sorted[i].Symbol)
		sj := strings.ToUpper(sorted[j].Symbol)
		if si == sj {
			return sorted[i].Side < sorted[j].Side
		}
		return si < sj
	})

	for _, entry := range sorted {
		symbol := strings.ToUpper(entry.Symbol)
		qtyText := formatQuantity(entry.Qty)
		if sr, ok := srMap[symbol]; ok {
			longStatus := "L❌"
			if sr.LongSignal {
				longStatus = "L✅"
			}
			shortStatus := "S❌"
			if sr.ShortSignal {
				shortStatus = "S✅"
			}
			b.WriteString(fmt.Sprintf(
				"%-12s %-5s Qty:%s %s/%s Last:%0.6f Score:%0.2f | %s | %s 30m:%+0.2f%% 10m:%+0.2f%%\n",
				symbol,
				entry.Side,
				qtyText,
				longStatus,
				shortStatus,
				sr.Metrics.Last,
				sr.Score,
				summarizeTimeframe(sr.FiveMinute),
				summarizeTimeframe(sr.ThreeMinute),
				sr.Metrics.Change30m,
				sr.Metrics.Change10m,
			))
		} else {
			b.WriteString(fmt.Sprintf("%-12s %-5s Qty:%s 指标数据不足，等待最新K线\n", symbol, entry.Side, qtyText))
		}
	}
	return strings.TrimSpace(b.String())
}

func sendTelegramMessages(ctx context.Context, c *http.Client, token, chatID, text string) error {
	const maxChunkRunes = 3500
	runes := []rune(text)
	for len(runes) > 0 {
		chunkLen := len(runes)
		if chunkLen > maxChunkRunes {
			chunkLen = maxChunkRunes
			// 尽量在换行处分割，避免拆断内容
			for chunkLen > 0 && runes[chunkLen-1] != '\n' {
				chunkLen--
			}
			if chunkLen == 0 {
				chunkLen = maxChunkRunes
			}
		}
		chunk := strings.TrimSpace(string(runes[:chunkLen]))
		if err := sendTelegramMessage(ctx, c, token, chatID, chunk); err != nil {
			return err
		}
		runes = runes[chunkLen:]
		for len(runes) > 0 && runes[0] == '\n' {
			runes = runes[1:]
		}
	}
	return nil
}

func sendTelegramMessage(ctx context.Context, c *http.Client, token, chatID, text string) error {
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
	form := url.Values{}
	form.Set("chat_id", chatID)
	form.Set("text", text)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Telegram API 返回状态 %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func computeTimeframeAnalysis(interval string, candles []candle, cfg config) (timeframeAnalysis, error) {
	analysis := timeframeAnalysis{Interval: interval}
	clean := dedupeCandles(candles)
	if len(clean) == 0 {
		return analysis, errors.New("K线数据为空")
	}

	// Ensure enough data for EMA/MACD calculations
	required := cfg.emaSlowPeriod
	if cfg.macdSlowPeriod > required {
		required = cfg.macdSlowPeriod
	}
	if cfg.macdSignalPeriod > required {
		required = cfg.macdSignalPeriod
	}
	if len(clean) < required+1 {
		return analysis, errors.New("K线数据不足以计算指标")
	}

	closes := make([]float64, 0, len(clean))
	for _, c := range clean {
		closes = append(closes, c.Close)
	}

	emaFast := emaSeries(closes, cfg.emaFastPeriod)
	emaSlow := emaSeries(closes, cfg.emaSlowPeriod)
	if len(emaFast) == 0 || len(emaSlow) == 0 {
		return analysis, errors.New("EMA 数据不足")
	}
	lastIdx := len(closes) - 1
	if lastIdx < 1 {
		return analysis, errors.New("K线不足以获取EMA斜率")
	}

	fastCurrent := emaFast[lastIdx]
	fastPrev := emaFast[lastIdx-1]
	slowCurrent := emaSlow[lastIdx]
	slowPrev := emaSlow[lastIdx-1]

	macdFastSeries := emaSeries(closes, cfg.macdFastPeriod)
	macdSlowSeries := emaSeries(closes, cfg.macdSlowPeriod)
	if len(macdFastSeries) == 0 || len(macdSlowSeries) == 0 {
		return analysis, errors.New("MACD EMA 数据不足")
	}
	macdLine := make([]float64, len(closes))
	for i := range macdLine {
		macdLine[i] = macdFastSeries[i] - macdSlowSeries[i]
	}
	macdSignalSeries := emaSeries(macdLine, cfg.macdSignalPeriod)
	if len(macdSignalSeries) == 0 {
		return analysis, errors.New("MACD 信号线数据不足")
	}
	macdCurrent := macdLine[lastIdx]
	macdPrev := macdLine[lastIdx-1]
	signalCurrent := macdSignalSeries[lastIdx]
	signalPrev := macdSignalSeries[lastIdx-1]
	macdHist := macdCurrent - signalCurrent
	macdHistPrev := macdPrev - signalPrev

	adxVal, err := computeADX(clean, cfg.adxPeriod)
	if err != nil {
		return analysis, err
	}

	analysis.EMAFast = fastCurrent
	analysis.EMASlow = slowCurrent
	analysis.EMAFastSlope = fastCurrent - fastPrev
	analysis.EMASlowSlope = slowCurrent - slowPrev
	analysis.MACDHist = macdHist
	analysis.MACDPrev = macdHistPrev
	analysis.ADX = adxVal
	analysis.LongSignal = fastCurrent > slowCurrent && analysis.EMAFastSlope > 0 && analysis.EMASlowSlope > 0 && macdHist > 0 && macdHist > macdHistPrev && adxVal > cfg.adxThreshold
	analysis.ShortSignal = fastCurrent < slowCurrent && analysis.EMAFastSlope < 0 && analysis.EMASlowSlope < 0 && macdHist < 0 && macdHist < macdHistPrev && adxVal > cfg.adxThreshold
	return analysis, nil
}

func scoreTimeframe(tf timeframeAnalysis, adxThreshold float64, long bool) float64 {
	score := 0.0
	adxBoost := tf.ADX - adxThreshold
	if adxBoost > 0 {
		score += adxBoost / 10.0
	}
	if long {
		if diff := tf.EMAFast - tf.EMASlow; diff > 0 {
			score += diff
		}
		if tf.EMAFastSlope > 0 {
			score += tf.EMAFastSlope
		}
		if tf.EMASlowSlope > 0 {
			score += tf.EMASlowSlope
		}
		if tf.MACDHist > 0 {
			score += tf.MACDHist
		}
		if growth := tf.MACDHist - tf.MACDPrev; growth > 0 {
			score += growth
		}
	} else {
		if diff := tf.EMASlow - tf.EMAFast; diff > 0 {
			score += diff
		}
		if tf.EMAFastSlope < 0 {
			score += -tf.EMAFastSlope
		}
		if tf.EMASlowSlope < 0 {
			score += -tf.EMASlowSlope
		}
		if tf.MACDHist < 0 {
			score += -tf.MACDHist
		}
		if growth := tf.MACDPrev - tf.MACDHist; growth > 0 {
			score += growth
		}
	}
	return score
}

func computeStrategyFromCandles(symbol string, candles []candle, cfg config) (strategyResult, error) {
	result := strategyResult{Symbol: symbol}
	clean := dedupeCandles(candles)
	if len(clean) < 121 {
		return result, errors.New("1m K线不足 121 条")
	}

	metrics, err := computeMetricsFromCandles(symbol, clean)
	if err != nil {
		return result, err
	}
	result.Metrics = metrics

	threeMinuteCandles := aggregateToInterval(clean, 3*time.Minute)
	if len(threeMinuteCandles) == 0 {
		return result, errors.New("3m K线数据不足")
	}
	threeMinute, err := computeTimeframeAnalysis("3m", threeMinuteCandles, cfg)
	if err != nil {
		return result, err
	}

	fiveMinuteCandles := aggregateTo5m(clean)
	fiveMinute, err := computeTimeframeAnalysis("5m", fiveMinuteCandles, cfg)
	if err != nil {
		return result, err
	}

	result.ThreeMinute = threeMinute
	result.FiveMinute = fiveMinute
	result.LongSignal = threeMinute.LongSignal && fiveMinute.LongSignal
	result.ShortSignal = threeMinute.ShortSignal && fiveMinute.ShortSignal
	if result.LongSignal {
		score3m := scoreTimeframe(threeMinute, cfg.adxThreshold, true)
		score5m := scoreTimeframe(fiveMinute, cfg.adxThreshold, true)
		result.Score = score3m*1.2 + score5m
	} else if result.ShortSignal {
		score3m := scoreTimeframe(threeMinute, cfg.adxThreshold, false)
		score5m := scoreTimeframe(fiveMinute, cfg.adxThreshold, false)
		result.Score = score3m*1.2 + score5m
	} else {
		result.Score = 0
	}
	return result, nil
}

func fetchSymbolMetrics(ctx context.Context, c *http.Client, symbol string) (symbolMetrics, error) {
	metrics := symbolMetrics{Symbol: symbol}
	candles, err := fetchHistorical1m(ctx, c, symbol, positionKlineLimit)
	if err != nil {
		return metrics, err
	}
	if len(candles) == 0 {
		return metrics, errors.New("K线数据不足")
	}
	return computeMetricsFromCandles(symbol, candles)
}

func computeMetricsFromCandles(symbol string, candles []candle) (symbolMetrics, error) {
	metrics := symbolMetrics{Symbol: symbol}
	if len(candles) < 121 {
		return metrics, errors.New("K线数据不足")
	}
	clean := dedupeCandles(candles)
	if len(clean) < 121 {
		return metrics, errors.New("K线数据不足")
	}
	metrics.Last = clean[len(clean)-1].Close
	highs := make([]float64, len(clean))
	lows := make([]float64, len(clean))
	for i, c := range clean {
		highs[i] = c.High
		lows[i] = c.Low
	}
	calcChange := func(minutes int) (float64, error) {
		target := clean[len(clean)-1].CloseTime.Add(-time.Duration(minutes) * time.Minute)
		for i := len(clean) - 1; i >= 0; i-- {
			if !clean[i].CloseTime.After(target) {
				prev := clean[i].Close
				if prev == 0 {
					return 0, errors.New("前值为0")
				}
				return (metrics.Last - prev) / prev * 100.0, nil
			}
		}
		return 0, errors.New("数据不足")
	}
	var err error
	if metrics.Change10m, err = calcChange(10); err != nil {
		return metrics, err
	}
	if metrics.Change30m, err = calcChange(30); err != nil {
		return metrics, err
	}
	if metrics.Change60m, err = calcChange(60); err != nil {
		return metrics, err
	}
	if metrics.Change120m, err = calcChange(120); err != nil {
		return metrics, err
	}
	metrics.AvgAmplitude = calculateAverageAmplitude(highs, lows, 10)
	return metrics, nil
}

func aggregateToInterval(candles []candle, interval time.Duration) []candle {
	if len(candles) == 0 {
		return nil
	}
	aggregated := make([]candle, 0, len(candles)/int(interval/time.Minute)+1)
	var current candle
	var bucket time.Time
	var count int
	var lastClose time.Time
	for _, c := range candles {
		b := c.OpenTime.Truncate(interval)
		if count == 0 || !b.Equal(bucket) {
			if count > 0 && !lastClose.Before(bucket.Add(interval)) {
				aggregated = append(aggregated, current)
			}
			bucket = b
			current = candle{
				OpenTime:  b,
				CloseTime: b.Add(interval),
				Open:      c.Open,
				High:      c.High,
				Low:       c.Low,
				Close:     c.Close,
				Volume:    c.Volume,
			}
			count = 1
			lastClose = c.CloseTime
			continue
		}
		if c.High > current.High {
			current.High = c.High
		}
		if c.Low < current.Low {
			current.Low = c.Low
		}
		current.Close = c.Close
		current.Volume += c.Volume
		lastClose = c.CloseTime
		count++
	}
	if count > 0 && !lastClose.Before(bucket.Add(interval)) {
		aggregated = append(aggregated, current)
	}
	return aggregated
}

func aggregateTo5m(candles []candle) []candle {
	return aggregateToInterval(candles, 5*time.Minute)
}

func emaSeries(values []float64, period int) []float64 {
	if period <= 0 || len(values) < period {
		return nil
	}
	ema := make([]float64, len(values))
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += values[i]
	}
	ema[period-1] = sum / float64(period)
	multiplier := 2.0 / float64(period+1)
	for i := period; i < len(values); i++ {
		ema[i] = (values[i]-ema[i-1])*multiplier + ema[i-1]
	}
	for i := 0; i < period-1; i++ {
		ema[i] = ema[period-1]
	}
	return ema
}

func computeADX(candles []candle, period int) (float64, error) {
	if period <= 0 {
		return 0, errors.New("ADX 周期需大于0")
	}
	if len(candles) < period+1 {
		return 0, errors.New("5m K线数据不足")
	}
	trs := make([]float64, len(candles))
	plusDM := make([]float64, len(candles))
	minusDM := make([]float64, len(candles))
	for i := 1; i < len(candles); i++ {
		highDiff := candles[i].High - candles[i-1].High
		lowDiff := candles[i-1].Low - candles[i].Low
		if highDiff > 0 && highDiff > lowDiff {
			plusDM[i] = highDiff
		}
		if lowDiff > 0 && lowDiff > highDiff {
			minusDM[i] = lowDiff
		}
		highLow := candles[i].High - candles[i].Low
		highClose := math.Abs(candles[i].High - candles[i-1].Close)
		lowClose := math.Abs(candles[i].Low - candles[i-1].Close)
		trs[i] = math.Max(math.Max(highLow, highClose), lowClose)
	}

	trSmooth := 0.0
	plusSmooth := 0.0
	minusSmooth := 0.0
	for i := 1; i <= period && i < len(candles); i++ {
		trSmooth += trs[i]
		plusSmooth += plusDM[i]
		minusSmooth += minusDM[i]
	}
	if trSmooth == 0 {
		return 0, errors.New("TR 为0")
	}
	plusDI := 100 * (plusSmooth / trSmooth)
	minusDI := 100 * (minusSmooth / trSmooth)
	denom := plusDI + minusDI
	if denom == 0 {
		return 0, errors.New("DI 计算异常")
	}
	dx := 100 * math.Abs(plusDI-minusDI) / denom
	dxs := []float64{dx}

	for i := period + 1; i < len(candles); i++ {
		trSmooth = trSmooth - trSmooth/float64(period) + trs[i]
		plusSmooth = plusSmooth - plusSmooth/float64(period) + plusDM[i]
		minusSmooth = minusSmooth - minusSmooth/float64(period) + minusDM[i]
		if trSmooth == 0 {
			continue
		}
		plusDI = 100 * (plusSmooth / trSmooth)
		minusDI = 100 * (minusSmooth / trSmooth)
		denom = plusDI + minusDI
		if denom == 0 {
			continue
		}
		dx = 100 * math.Abs(plusDI-minusDI) / denom
		dxs = append(dxs, dx)
	}

	if len(dxs) < period {
		return 0, errors.New("DX 数据不足")
	}
	adx := 0.0
	for i := 0; i < period; i++ {
		adx += dxs[i]
	}
	adx /= float64(period)
	for i := period; i < len(dxs); i++ {
		adx = ((adx * float64(period-1)) + dxs[i]) / float64(period)
	}
	return adx, nil
}

func calculateAverageAmplitude(highs, lows []float64, window int) float64 {
	if len(highs) == 0 || len(lows) == 0 || window <= 0 {
		return 0
	}
	if window > len(highs) {
		window = len(highs)
	}
	start := len(highs) - window
	amplitudes := make([]float64, 0, window)
	for i := start; i < len(highs); i++ {
		h := highs[i]
		l := lows[i]
		if l <= 0 || h < l {
			continue
		}
		amp := (h - l) / l * 100.0
		amplitudes = append(amplitudes, amp)
	}
	if len(amplitudes) == 0 {
		return 0
	}
	return trimmedAverage(amplitudes, 2)
}

func trimmedAverage(values []float64, trim int) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) <= trim*2 {
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	trimmed := sorted[trim : len(sorted)-trim]
	sum := 0.0
	for _, v := range trimmed {
		sum += v
	}
	return sum / float64(len(trimmed))
}

func getJSON(ctx context.Context, c *http.Client, url string, v any) error {
	// 简单的重试策略
	var lastErr error
	for i := 0; i < 3; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "10mshake/1.0 (+https://binance.com)")
		resp, err := c.Do(req)
		if err != nil {
			lastErr = err
		} else {
			if resp.Body != nil {
				defer resp.Body.Close()
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				dec := json.NewDecoder(resp.Body)
				if err := dec.Decode(v); err != nil {
					lastErr = err
				} else {
					return nil
				}
			} else {
				lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			}
		}
		// 指数退避
		time.Sleep(time.Duration(200*(1<<i)) * time.Millisecond)
	}
	return lastErr
}
