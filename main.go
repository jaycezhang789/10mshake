package main

import (
	"bufio"
	"bytes"
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
	initialKlineLimit           = 499
	positionKlineLimit          = 499
	macdStdMultiplier           = 0.15
	sepEnterThreshold           = 0.8
	sepExitThreshold            = 0.5
	reentryResetATRMultiplier   = 0.4
	reentryReentryATRMultiplier = 0.6
	minBarsHeld                 = 3
	reentryCooldownBars         = 6
	choppinessThreshold         = 61.0
	choppinessAdxMax            = 18.0
	spreadAtrThreshold          = 0.07
	btcVolMultiplier            = 3.0
	btcCooloffDuration          = 15 * time.Minute
)

var (
	minHoldDuration = time.Duration(minBarsHeld) * 3 * time.Minute
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
	Interval      string
	EMAFast       float64
	EMASlow       float64
	EMAFastSlope  float64
	EMASlowSlope  float64
	MACDHist      float64
	MACDPrev      float64
	MACDPrevPrev  float64
	ADX           float64
	ADXPrev       float64
	ADXPrev2      float64
	ADXPrev3      float64
	MACDStd200    float64
	MACDEpsilon   float64
	ATR50         float64
	ATR22         float64
	HighestHigh22 float64
	LowestLow22   float64
	Choppiness    float64
	Sep           float64
	LongSignal    bool
	ShortSignal   bool
}

type strategyResult struct {
	Symbol        string
	Metrics       symbolMetrics
	ThreeMinute   timeframeAnalysis
	FiveMinute    timeframeAnalysis
	LongSignal    bool
	ShortSignal   bool
	Score         float64
	Last3mBarID   string
	Last5mBarID   string
	EntryBlocked  bool
	BlockReasons  []string
	SpreadRatio   float64
	DepthNotional float64
}

type intervalWatcher struct {
	symbol   string
	interval string
	duration time.Duration
	mu       sync.RWMutex
	candles  []candle
	onCandle func(string, string)
}

type intervalHub struct {
	interval    string
	mgr         *symbolManager
	ctx         context.Context
	cancel      context.CancelFunc
	subscribe   chan string
	unsubscribe chan string
	connMu      sync.Mutex
	conn        *websocket.Conn
	commandMu   sync.Mutex
	nextID      int64
}

type combinedStreamEvent struct {
	Stream string       `json:"stream"`
	Data   wsKlineEvent `json:"data"`
}

type symbolManager struct {
	mu       sync.RWMutex
	watchers map[string]map[string]*intervalWatcher
	hubMu    sync.RWMutex
	hubs     map[string]*intervalHub
	client   *http.Client
	cfg      config
	onCandle func(string, string)
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

var intervalConfigs = []struct {
	Name     string
	Duration time.Duration
}{
	{"3m", 3 * time.Minute},
	{"5m", 5 * time.Minute},
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
	watchers := make(map[string]map[string]*intervalWatcher)
	for _, iv := range intervalConfigs {
		watchers[iv.Name] = make(map[string]*intervalWatcher)
	}
	mgr := &symbolManager{
		watchers: watchers,
		hubs:     make(map[string]*intervalHub),
		client:   client,
		cfg:      cfg,
		onCandle: nil,
	}
	for _, iv := range intervalConfigs {
		mgr.hubs[strings.ToLower(iv.Name)] = newIntervalHub(mgr, iv.Name)
	}
	return mgr
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

func newIntervalWatcher(symbol, interval string, duration time.Duration, candles []candle, handler func(string, string)) *intervalWatcher {
	clean := dedupeCandles(candles)
	copyCandles := make([]candle, len(clean))
	copy(copyCandles, clean)
	return &intervalWatcher{
		symbol:   symbol,
		interval: strings.ToLower(interval),
		duration: duration,
		candles:  copyCandles,
		onCandle: handler,
	}
}

func (w *intervalWatcher) setHandler(handler func(string, string)) {
	w.mu.Lock()
	w.onCandle = handler
	w.mu.Unlock()
}

func newIntervalHub(mgr *symbolManager, interval string) *intervalHub {
	ctx, cancel := context.WithCancel(context.Background())
	hub := &intervalHub{
		interval:    strings.ToLower(interval),
		mgr:         mgr,
		ctx:         ctx,
		cancel:      cancel,
		subscribe:   make(chan string, 512),
		unsubscribe: make(chan string, 512),
	}
	go hub.run()
	return hub
}

func (h *intervalHub) run() {
	backoff := time.Second
	for {
		if h.ctx.Err() != nil {
			return
		}
		conn, _, err := websocket.DefaultDialer.DialContext(h.ctx, wsBaseURL+"/stream", nil)
		if err != nil {
			log.Printf("interval hub %s 连接失败: %v", h.interval, err)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			continue
		}
		conn.SetReadLimit(1 << 20)
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})
		h.connMu.Lock()
		h.conn = conn
		h.connMu.Unlock()
		backoff = time.Second
		h.resubscribeAll(conn)
		errCh := make(chan error, 2)
		go h.writer(conn, errCh)
		go h.reader(conn, errCh)
		select {
		case <-h.ctx.Done():
			conn.Close()
			h.connMu.Lock()
			h.conn = nil
			h.connMu.Unlock()
			return
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("interval hub %s 连接断开: %v", h.interval, err)
			}
			conn.Close()
			h.connMu.Lock()
			h.conn = nil
			h.connMu.Unlock()
		}
	}
}

func (h *intervalHub) writer(conn *websocket.Conn, errCh chan<- error) {
	pingTicker := time.NewTicker(15 * time.Second)
	defer pingTicker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case sym := <-h.subscribe:
			if err := h.sendCommand(conn, "SUBSCRIBE", []string{strings.ToLower(sym) + "@kline_" + h.interval}); err != nil {
				errCh <- err
				return
			}
		case sym := <-h.unsubscribe:
			if err := h.sendCommand(conn, "UNSUBSCRIBE", []string{strings.ToLower(sym) + "@kline_" + h.interval}); err != nil {
				errCh <- err
				return
			}
		case <-pingTicker.C:
			if err := conn.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
				errCh <- err
				return
			}
			if err := conn.WriteMessage(websocket.PingMessage, []byte("ping")); err != nil {
				errCh <- err
				return
			}
		}
	}
}

func (h *intervalHub) reader(conn *websocket.Conn, errCh chan<- error) {
	for {
		if h.ctx.Err() != nil {
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			errCh <- err
			return
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		if len(data) == 0 {
			continue
		}
		if bytes.Equal(data, []byte("pong")) {
			continue
		}
		if bytes.Contains(data, []byte("\"result\"")) {
			continue
		}
		var evt combinedStreamEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			continue
		}
		candle, err := evt.Data.toCandle()
		if err != nil {
			continue
		}
		symbol := strings.ToUpper(evt.Data.Symbol)
		if symbol == "" && evt.Stream != "" {
			parts := strings.Split(evt.Stream, "@")
			if len(parts) > 0 {
				symbol = strings.ToUpper(parts[0])
			}
		}
		if symbol == "" {
			continue
		}
		h.dispatch(symbol, candle)
	}
}

func (h *intervalHub) dispatch(symbol string, candle candle) {
	watcher := h.mgr.getWatcher(symbol, strings.ToLower(h.interval))
	if watcher == nil {
		return
	}
	watcher.upsert(candle)
}

func (h *intervalHub) sendCommand(conn *websocket.Conn, method string, params []string) error {
	if conn == nil {
		h.connMu.Lock()
		conn = h.conn
		h.connMu.Unlock()
		if conn == nil {
			return nil
		}
	}
	payload := map[string]interface{}{
		"method": strings.ToUpper(method),
		"params": params,
	}
	h.commandMu.Lock()
	h.nextID++
	payload["id"] = h.nextID
	if err := conn.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
		h.commandMu.Unlock()
		return err
	}
	err := conn.WriteJSON(payload)
	h.commandMu.Unlock()
	return err
}

func (h *intervalHub) resubscribeAll(conn *websocket.Conn) {
	h.mgr.mu.RLock()
	intervalMap := h.mgr.watchers[strings.ToLower(h.interval)]
	symbols := make([]string, 0, len(intervalMap))
	for sym := range intervalMap {
		symbols = append(symbols, sym)
	}
	h.mgr.mu.RUnlock()
	for _, sym := range symbols {
		_ = h.sendCommand(conn, "SUBSCRIBE", []string{strings.ToLower(sym) + "@kline_" + h.interval})
	}
}

func (h *intervalHub) addSymbol(symbol string) {
	select {
	case h.subscribe <- symbol:
	default:
		go func() { h.subscribe <- symbol }()
	}
}

func (h *intervalHub) removeSymbol(symbol string) {
	select {
	case h.unsubscribe <- symbol:
	default:
		go func() { h.unsubscribe <- symbol }()
	}
}

func (h *intervalHub) Close() {
	h.cancel()
	h.connMu.Lock()
	if h.conn != nil {
		h.conn.Close()
		h.conn = nil
	}
	h.connMu.Unlock()
}

func (w *intervalWatcher) upsert(c candle) {
	w.mu.Lock()
	handler := w.onCandle
	updated := false
	n := len(w.candles)
	if n > 0 && w.candles[n-1].OpenTime.Equal(c.OpenTime) {
		w.candles[n-1] = c
		updated = true
	} else {
		w.candles = append(w.candles, c)
		if len(w.candles) > positionKlineLimit {
			w.candles = w.candles[len(w.candles)-positionKlineLimit:]
		}
		updated = true
	}
	w.mu.Unlock()
	if updated && handler != nil {
		go handler(w.symbol, w.interval)
	}
}

func (w *intervalWatcher) snapshot() []candle {
	w.mu.RLock()
	defer w.mu.RUnlock()
	copyCandles := make([]candle, len(w.candles))
	copy(copyCandles, w.candles)
	return dedupeCandles(copyCandles)
}

func (w *intervalWatcher) Close() {}

func (m *symbolManager) EnsureWatchers(ctx context.Context, symbols []string) {
	for _, sym := range symbols {
		sym = strings.ToUpper(sym)
		if sym == "" {
			continue
		}
		for _, iv := range intervalConfigs {
			m.ensureIntervalWatcher(ctx, sym, iv.Name, iv.Duration)
		}
	}
}

func (m *symbolManager) ensureIntervalWatcher(ctx context.Context, symbol, interval string, duration time.Duration) {
	m.mu.RLock()
	intervalMap := m.watchers[interval]
	existing := intervalMap[strings.ToUpper(symbol)]
	onCandle := m.onCandle
	m.mu.RUnlock()
	if existing != nil {
		existing.setHandler(onCandle)
		return
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	candles, err := fetchHistoricalInterval(ctxTimeout, m.client, symbol, interval, initialKlineLimit)
	cancel()
	if err != nil {
		log.Printf("初始化 %s %s 监听失败: %v", symbol, interval, err)
		return
	}
	watcher := newIntervalWatcher(symbol, interval, duration, candles, onCandle)
	m.mu.Lock()
	intervalMap = m.watchers[interval]
	if _, exists := intervalMap[strings.ToUpper(symbol)]; exists {
		m.mu.Unlock()
		return
	}
	intervalMap[strings.ToUpper(symbol)] = watcher
	m.mu.Unlock()
	if hub := m.getHub(interval); hub != nil {
		hub.addSymbol(symbol)
	}
	log.Printf("开始监听 %s %s，初始K线: %d 条", symbol, interval, len(candles))
}

func (m *symbolManager) getWatcher(symbol, interval string) *intervalWatcher {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if intervalMap, ok := m.watchers[interval]; ok {
		return intervalMap[strings.ToUpper(symbol)]
	}
	return nil
}

func (m *symbolManager) getHub(interval string) *intervalHub {
	m.hubMu.RLock()
	defer m.hubMu.RUnlock()
	return m.hubs[strings.ToLower(interval)]
}

func (m *symbolManager) SetCandleHandler(handler func(string, string)) {
	m.mu.Lock()
	m.onCandle = handler
	for interval := range m.watchers {
		for _, w := range m.watchers[interval] {
			w.setHandler(handler)
		}
	}
	m.mu.Unlock()
}

func (m *symbolManager) EvaluateStrategies(symbols []string) []strategyResult {
	results := make([]strategyResult, 0, len(symbols))
	for _, sym := range symbols {
		w3 := m.getWatcher(sym, "3m")
		w5 := m.getWatcher(sym, "5m")
		if w3 == nil || w5 == nil {
			continue
		}
		candles3 := w3.snapshot()
		candles5 := w5.snapshot()
		if len(candles3) == 0 || len(candles5) == 0 {
			continue
		}
		rs, err := computeStrategyFromCandles(sym, candles3, candles5, m.cfg)
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
	for _, intervalMap := range m.watchers {
		for k := range intervalMap {
			delete(intervalMap, k)
		}
	}
	m.mu.Unlock()
	m.hubMu.RLock()
	hubs := make([]*intervalHub, 0, len(m.hubs))
	for _, hub := range m.hubs {
		hubs = append(hubs, hub)
	}
	m.hubMu.RUnlock()
	for _, hub := range hubs {
		hub.Close()
	}
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

type reentryState struct {
	State             string
	LastExitPrice     float64
	LastExitTime      time.Time
	LastExitType      string
	CooldownRemaining int
}

const (
	reentryStateNone  = ""
	reentryStateExit  = "exit"
	reentryStateReset = "reset"
)

const (
	exitTypeSoft = "soft"
	exitTypeHard = "hard"
)

type trailingStop struct {
	CE          float64
	Peak        float64
	InitialRisk float64
	Multiplier  float64
	LastUpdated time.Time
}

type candleSnapshot struct {
	Strategy strategyResult
	Last3ID  string
	Last5ID  string
	Created  time.Time
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

func (c *binanceClient) FetchDepth(ctx context.Context, symbol string) (bestBid, bestAsk, bidNotional, askNotional float64, err error) {
	params := url.Values{}
	params.Set("symbol", strings.ToUpper(symbol))
	params.Set("limit", "10")
	body, err := c.publicRequest(ctx, "/fapi/v1/depth", params)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	var resp struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, 0, 0, 0, err
	}
	if len(resp.Bids) == 0 || len(resp.Asks) == 0 {
		return 0, 0, 0, 0, fmt.Errorf("orderbook 数据不足")
	}
	parseLevel := func(level []string) (float64, float64) {
		if len(level) < 2 {
			return 0, 0
		}
		price, _ := strconv.ParseFloat(level[0], 64)
		qty, _ := strconv.ParseFloat(level[1], 64)
		return price, qty
	}
	for i, level := range resp.Bids {
		price, qty := parseLevel(level)
		if i == 0 {
			bestBid = price
		}
		bidNotional += price * qty
	}
	for i, level := range resp.Asks {
		price, qty := parseLevel(level)
		if i == 0 {
			bestAsk = price
		}
		askNotional += price * qty
	}
	return
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
	client           *binanceClient
	positions        *positionManager
	cfg              config
	qtyMu            sync.RWMutex
	qty              map[string]float64
	httpClient       *http.Client
	watchMgr         *symbolManager
	cooldownMu       sync.RWMutex
	cooldown         map[string]int
	tightenMu        sync.RWMutex
	tightened        map[string]bool
	reentryMu        sync.RWMutex
	reentry          map[string]reentryState
	entryMu          sync.RWMutex
	entry            map[string]time.Time
	trailingMu       sync.RWMutex
	trailing         map[string]trailingStop
	volMu            sync.Mutex
	btcCooloffUntil  time.Time
	lastBtcCheck     time.Time
	snapshotMu       sync.RWMutex
	snapshots        map[string]candleSnapshot
	lastEvalMu       sync.Mutex
	lastEvaluated    map[string]string
	recentlyClosedMu sync.RWMutex
	recentlyClosed   map[string]time.Time
}

func newTradeManager(client *binanceClient, positions []positionEntry, cfg config, httpClient *http.Client, watchMgr *symbolManager) *tradeManager {
	pm := newPositionManager(cfg.maxPositions)
	pm.Replace(positions)
	return &tradeManager{
		client:         client,
		positions:      pm,
		cfg:            cfg,
		qty:            make(map[string]float64),
		httpClient:     httpClient,
		watchMgr:       watchMgr,
		cooldown:       make(map[string]int),
		tightened:      make(map[string]bool),
		reentry:        make(map[string]reentryState),
		entry:          make(map[string]time.Time),
		trailing:       make(map[string]trailingStop),
		snapshots:      make(map[string]candleSnapshot),
		lastEvaluated:  make(map[string]string),
		recentlyClosed: make(map[string]time.Time),
	}
}

func (tm *tradeManager) tightenKey(symbol, side string) string {
	return strings.ToUpper(symbol) + "::" + strings.ToUpper(side)
}

func (tm *tradeManager) setTightening(symbol, side string) {
	tm.tightenMu.Lock()
	if tm.tightened == nil {
		tm.tightened = make(map[string]bool)
	}
	tm.tightened[tm.tightenKey(symbol, side)] = true
	tm.tightenMu.Unlock()
}

func (tm *tradeManager) clearTightening(symbol, side string) {
	tm.tightenMu.Lock()
	if tm.tightened != nil {
		delete(tm.tightened, tm.tightenKey(symbol, side))
	}
	tm.tightenMu.Unlock()
	tm.clearTrailing(symbol, side)
}

func (tm *tradeManager) isTightening(symbol, side string) bool {
	tm.tightenMu.RLock()
	defer tm.tightenMu.RUnlock()
	if tm.tightened == nil {
		return false
	}
	return tm.tightened[tm.tightenKey(symbol, side)]
}

func (tm *tradeManager) reentryKey(symbol, side string) string {
	return strings.ToUpper(symbol) + "::" + strings.ToUpper(side)
}

func (tm *tradeManager) markReentryExit(symbol, side string, price float64, exitType string) {
	if strings.ToUpper(side) != "LONG" && strings.ToUpper(side) != "SHORT" {
		return
	}
	tm.markRecentlyClosed(symbol, side)
	tm.reentryMu.Lock()
	if tm.reentry == nil {
		tm.reentry = make(map[string]reentryState)
	}
	cooldown := reentryCooldownBars
	if cooldown < 0 {
		cooldown = 0
	}
	tm.reentry[tm.reentryKey(symbol, side)] = reentryState{
		State:             reentryStateExit,
		LastExitPrice:     price,
		LastExitTime:      time.Now(),
		LastExitType:      exitType,
		CooldownRemaining: cooldown,
	}
	tm.reentryMu.Unlock()
}

func (tm *tradeManager) clearReentryState(symbol, side string) {
	tm.reentryMu.Lock()
	if tm.reentry != nil {
		delete(tm.reentry, tm.reentryKey(symbol, side))
	}
	tm.reentryMu.Unlock()
	tm.clearRecentlyClosed(symbol, side)
}

func (tm *tradeManager) recentlyClosedKey(symbol, side string) string {
	return strings.ToUpper(symbol) + "::" + strings.ToUpper(side)
}

func (tm *tradeManager) markRecentlyClosed(symbol, side string) {
	tm.recentlyClosedMu.Lock()
	if tm.recentlyClosed == nil {
		tm.recentlyClosed = make(map[string]time.Time)
	}
	tm.recentlyClosed[tm.recentlyClosedKey(symbol, side)] = time.Now()
	tm.recentlyClosedMu.Unlock()
}

func (tm *tradeManager) clearRecentlyClosed(symbol, side string) {
	tm.recentlyClosedMu.Lock()
	if tm.recentlyClosed != nil {
		delete(tm.recentlyClosed, tm.recentlyClosedKey(symbol, side))
	}
	tm.recentlyClosedMu.Unlock()
}

func (tm *tradeManager) isRecentlyClosed(symbol, side string) bool {
	tm.recentlyClosedMu.RLock()
	defer tm.recentlyClosedMu.RUnlock()
	if tm.recentlyClosed == nil {
		return false
	}
	_, ok := tm.recentlyClosed[tm.recentlyClosedKey(symbol, side)]
	return ok
}

func (tm *tradeManager) storeSnapshot(sr strategyResult) {
	if strings.TrimSpace(sr.Symbol) == "" {
		return
	}
	tm.snapshotMu.Lock()
	if tm.snapshots == nil {
		tm.snapshots = make(map[string]candleSnapshot)
	}
	tm.snapshots[strings.ToUpper(sr.Symbol)] = candleSnapshot{Strategy: sr, Last3ID: sr.Last3mBarID, Last5ID: sr.Last5mBarID, Created: time.Now()}
	tm.snapshotMu.Unlock()
}

func (tm *tradeManager) getSnapshot(symbol string) (strategyResult, bool) {
	tm.snapshotMu.RLock()
	defer tm.snapshotMu.RUnlock()
	if tm.snapshots == nil {
		return strategyResult{}, false
	}
	ss, ok := tm.snapshots[strings.ToUpper(symbol)]
	if !ok {
		return strategyResult{}, false
	}
	return ss.Strategy, true
}

func (tm *tradeManager) shouldEvaluateSymbolKey(symbol, key string) bool {
	tm.lastEvalMu.Lock()
	defer tm.lastEvalMu.Unlock()
	if tm.lastEvaluated == nil {
		tm.lastEvaluated = make(map[string]string)
	}
	symbol = strings.ToUpper(symbol)
	if prev, ok := tm.lastEvaluated[symbol]; ok && prev == key {
		return false
	}
	tm.lastEvaluated[symbol] = key
	return true
}

func (tm *tradeManager) updateReentryReset(symbol, side string, sr strategyResult) {
	if tm.positions != nil && tm.positions.Has(symbol, side) {
		tm.clearReentryState(symbol, side)
		return
	}
	tm.reentryMu.Lock()
	defer tm.reentryMu.Unlock()
	if tm.reentry == nil {
		tm.reentry = make(map[string]reentryState)
	}
	key := tm.reentryKey(symbol, side)
	state, ok := tm.reentry[key]
	if !ok || state.State != reentryStateExit {
		return
	}
	if tm.resetConditionMet(side, sr) {
		if state.State != reentryStateReset {
			state.State = reentryStateReset
			tm.reentry[key] = state
			log.Printf("%s %s 满足回撤确认，允许重入", symbol, side)
		}
	}
}

func (tm *tradeManager) resetConditionMet(side string, sr strategyResult) bool {
	price := sr.Metrics.Last
	atr := sr.ThreeMinute.ATR50
	epsilon := sr.ThreeMinute.MACDEpsilon
	if epsilon < 0 {
		epsilon = 0
	}
	switch strings.ToUpper(side) {
	case "LONG":
		priceTrigger := atr > 0 && price <= sr.ThreeMinute.EMAFast-reentryResetATRMultiplier*atr
		macdTrigger := sr.ThreeMinute.MACDHist <= -epsilon
		return macdTrigger || priceTrigger
	case "SHORT":
		priceTrigger := atr > 0 && price >= sr.ThreeMinute.EMAFast+reentryResetATRMultiplier*atr
		macdTrigger := sr.ThreeMinute.MACDHist >= epsilon
		return macdTrigger || priceTrigger
	default:
		return false
	}
}

func (tm *tradeManager) trailingKey(symbol, side string) string {
	return strings.ToUpper(symbol) + "::" + strings.ToUpper(side)
}

func chandelierMultiplier(sr strategyResult) float64 {
	adx := sr.FiveMinute.ADX
	sep := math.Abs(sr.FiveMinute.Sep)
	if adx >= 32 || sep >= 1.0 {
		return 2.8
	}
	return 2.1
}

func (tm *tradeManager) clearTrailing(symbol, side string) {
	tm.trailingMu.Lock()
	if tm.trailing != nil {
		delete(tm.trailing, tm.trailingKey(symbol, side))
	}
	tm.trailingMu.Unlock()
}

func (tm *tradeManager) getTrailing(symbol, side string) (trailingStop, bool) {
	tm.trailingMu.RLock()
	defer tm.trailingMu.RUnlock()
	if tm.trailing == nil {
		return trailingStop{}, false
	}
	ts, ok := tm.trailing[tm.trailingKey(symbol, side)]
	return ts, ok
}

func (tm *tradeManager) ensureTrailing(symbol, side string, sr strategyResult) {
	if sr.ThreeMinute.ATR22 <= 0 {
		return
	}
	mult := chandelierMultiplier(sr)
	price := sr.Metrics.Last
	var ce, peak, risk float64
	switch strings.ToUpper(side) {
	case "LONG":
		high := sr.ThreeMinute.HighestHigh22
		ce = high - mult*sr.ThreeMinute.ATR22
		peak = high
		risk = price - ce
	case "SHORT":
		low := sr.ThreeMinute.LowestLow22
		ce = low + mult*sr.ThreeMinute.ATR22
		peak = low
		risk = ce - price
	default:
		return
	}
	if risk <= 0 {
		risk = math.Abs(price) * 1e-4
	}
	tm.trailingMu.Lock()
	if tm.trailing == nil {
		tm.trailing = make(map[string]trailingStop)
	}
	key := tm.trailingKey(symbol, side)
	state, ok := tm.trailing[key]
	if !ok {
		tm.trailing[key] = trailingStop{CE: ce, Peak: peak, InitialRisk: risk, Multiplier: mult, LastUpdated: time.Now()}
		tm.trailingMu.Unlock()
		return
	}
	state.Multiplier = mult
	state.LastUpdated = time.Now()
	if strings.ToUpper(side) == "LONG" {
		if ce > state.CE {
			state.CE = ce
		}
		if peak > state.Peak {
			state.Peak = peak
		}
		if risk > state.InitialRisk {
			state.InitialRisk = risk
		}
	} else {
		if ce < state.CE {
			state.CE = ce
		}
		if peak < state.Peak || state.Peak == 0 {
			state.Peak = peak
		}
		if risk > state.InitialRisk {
			state.InitialRisk = risk
		}
	}
	tm.trailing[key] = state
	tm.trailingMu.Unlock()
}

func (tm *tradeManager) updateTrailing(symbol, side string, sr strategyResult) {
	if !tm.isTightening(symbol, side) {
		tm.clearTrailing(symbol, side)
		return
	}
	tm.ensureTrailing(symbol, side, sr)
}

func (tm *tradeManager) canEnterAfterReset(symbol, side string, sr strategyResult) bool {
	tm.reentryMu.RLock()
	state, ok := tm.reentry[tm.reentryKey(symbol, side)]
	tm.reentryMu.RUnlock()
	if !ok || state.State == reentryStateNone {
		return true
	}
	if state.State == reentryStateExit {
		return false
	}
	// reentryStateReset
	price := sr.Metrics.Last
	atr := sr.ThreeMinute.ATR50
	threshold := atr * reentryReentryATRMultiplier
	if atr <= 0 {
		threshold = 0
	}
	if state.CooldownRemaining > 0 {
		return false
	}
	if tm.isRecentlyClosed(symbol, side) && (state.State != reentryStateReset || state.CooldownRemaining > 0) {
		return false
	}
	switch strings.ToUpper(side) {
	case "LONG":
		minPrice := state.LastExitPrice + threshold
		if threshold <= 0 {
			minPrice = state.LastExitPrice * (1 + 1e-6)
		}
		if price <= minPrice {
			return false
		}
	case "SHORT":
		maxPrice := state.LastExitPrice - threshold
		if threshold <= 0 {
			maxPrice = state.LastExitPrice * (1 - 1e-6)
		}
		if price >= maxPrice {
			return false
		}
	default:
		return false
	}
	return true
}

func (tm *tradeManager) entryKey(symbol, side string) string {
	return strings.ToUpper(symbol) + "::" + strings.ToUpper(side)
}

func (tm *tradeManager) setEntryTime(symbol, side string, t time.Time) {
	tm.entryMu.Lock()
	if tm.entry == nil {
		tm.entry = make(map[string]time.Time)
	}
	tm.entry[tm.entryKey(symbol, side)] = t
	tm.entryMu.Unlock()
}

func (tm *tradeManager) clearEntryTime(symbol, side string) {
	tm.entryMu.Lock()
	if tm.entry != nil {
		delete(tm.entry, tm.entryKey(symbol, side))
	}
	tm.entryMu.Unlock()
}

func (tm *tradeManager) getEntryTime(symbol, side string) time.Time {
	tm.entryMu.RLock()
	defer tm.entryMu.RUnlock()
	if tm.entry == nil {
		return time.Time{}
	}
	if t, ok := tm.entry[tm.entryKey(symbol, side)]; ok {
		return t
	}
	return time.Time{}
}

func (tm *tradeManager) canSoftExit(symbol, side string) bool {
	if minBarsHeld <= 0 || minHoldDuration <= 0 {
		return true
	}
	entry := tm.getEntryTime(symbol, side)
	if entry.IsZero() {
		return true
	}
	now := time.Now()
	if entry.After(now) {
		return true
	}
	if now.Sub(entry) < minHoldDuration {
		return false
	}
	return true
}

func (tm *tradeManager) syncEntryTimes(entries []positionEntry) {
	tm.entryMu.Lock()
	defer tm.entryMu.Unlock()
	if tm.entry == nil {
		tm.entry = make(map[string]time.Time)
	}
	active := make(map[string]struct{}, len(entries))
	now := time.Now()
	for _, entry := range entries {
		key := tm.entryKey(entry.Symbol, entry.Side)
		active[key] = struct{}{}
		if _, ok := tm.entry[key]; !ok {
			if minHoldDuration > 0 {
				tm.entry[key] = now.Add(-minHoldDuration)
			} else {
				tm.entry[key] = now
			}
		}
	}
	for key := range tm.entry {
		if _, ok := active[key]; !ok {
			delete(tm.entry, key)
		}
	}
}

func (tm *tradeManager) tickReentryCooldown(symbol, interval string) {
	if strings.ToLower(interval) != "3m" {
		return
	}
	tm.reentryMu.Lock()
	defer tm.reentryMu.Unlock()
	if tm.reentry == nil {
		return
	}
	for _, side := range []string{"LONG", "SHORT"} {
		key := tm.reentryKey(symbol, side)
		state, ok := tm.reentry[key]
		if !ok || state.State == reentryStateNone || state.CooldownRemaining <= 0 {
			continue
		}
		state.CooldownRemaining--
		if state.CooldownRemaining < 0 {
			state.CooldownRemaining = 0
		}
		tm.reentry[key] = state
	}
}

func (tm *tradeManager) shouldChandelierExit(symbol, side string, sr strategyResult) bool {
	if !tm.isTightening(symbol, side) {
		return false
	}
	ts, ok := tm.getTrailing(symbol, side)
	if !ok {
		return false
	}
	price := sr.Metrics.Last
	switch strings.ToUpper(side) {
	case "LONG":
		return price <= ts.CE && ts.CE > 0
	case "SHORT":
		return price >= ts.CE && ts.CE > 0
	default:
		return false
	}
}

func (tm *tradeManager) shouldConfirmExit(symbol, side string, sr strategyResult) bool {
	if !tm.isTightening(symbol, side) {
		return false
	}
	ts, ok := tm.getTrailing(symbol, side)
	if !ok {
		return false
	}
	atr := sr.ThreeMinute.ATR22
	if atr <= 0 {
		atr = sr.ThreeMinute.ATR50
	}
	price := sr.Metrics.Last
	initialRisk := ts.InitialRisk
	if initialRisk <= 0 {
		initialRisk = atr
	}
	if initialRisk <= 0 {
		initialRisk = math.Abs(price) * 1e-4
	}
	atrDrawdown := 1.8 * atr
	profitDrawdownThreshold := 0.8 * initialRisk
	sep := sr.FiveMinute.Sep
	switch strings.ToUpper(side) {
	case "LONG":
		drawdown := ts.Peak - price
		if drawdown < 0 {
			drawdown = 0
		}
		if atrDrawdown > 0 && drawdown >= atrDrawdown {
			return true
		}
		if profitDrawdownThreshold > 0 && drawdown >= profitDrawdownThreshold {
			return true
		}
		if sep <= sepExitThreshold {
			return true
		}
	case "SHORT":
		drawdown := price - ts.Peak
		if drawdown < 0 {
			drawdown = 0
		}
		if atrDrawdown > 0 && drawdown >= atrDrawdown {
			return true
		}
		if profitDrawdownThreshold > 0 && drawdown >= profitDrawdownThreshold {
			return true
		}
		if sep >= -sepExitThreshold {
			return true
		}
	default:
		return false
	}
	return false
}

func shouldTightenPosition(side string, sr strategyResult) bool {
	epsilon := sr.ThreeMinute.MACDEpsilon
	if epsilon < 0 {
		epsilon = 0
	}
	enterSep := sepEnterThreshold
	switch strings.ToUpper(side) {
	case "LONG":
		weakening := sr.ThreeMinute.MACDHist > epsilon &&
			sr.ThreeMinute.MACDPrev > epsilon &&
			sr.ThreeMinute.MACDPrevPrev > epsilon &&
			sr.ThreeMinute.MACDHist < sr.ThreeMinute.MACDPrev &&
			sr.ThreeMinute.MACDPrev < sr.ThreeMinute.MACDPrevPrev
		sepCooling := sr.FiveMinute.Sep < enterSep
		return weakening && sepCooling
	case "SHORT":
		weakening := sr.ThreeMinute.MACDHist < -epsilon &&
			sr.ThreeMinute.MACDPrev < -epsilon &&
			sr.ThreeMinute.MACDPrevPrev < -epsilon &&
			sr.ThreeMinute.MACDHist > sr.ThreeMinute.MACDPrev &&
			sr.ThreeMinute.MACDPrev > sr.ThreeMinute.MACDPrevPrev
		sepCooling := sr.FiveMinute.Sep > -enterSep
		return weakening && sepCooling
	default:
		return false
	}
}

func shouldBaselineExit(side string, sr strategyResult) bool {
	epsilon := sr.ThreeMinute.MACDEpsilon
	if epsilon < 0 {
		epsilon = 0
	}
	exitSep := sepExitThreshold
	switch strings.ToUpper(side) {
	case "LONG":
		histExit := sr.ThreeMinute.MACDHist < -epsilon
		sepExit := sr.FiveMinute.Sep <= exitSep && sr.FiveMinute.EMAFastSlope <= 0
		return histExit || sepExit
	case "SHORT":
		histExit := sr.ThreeMinute.MACDHist > epsilon
		sepExit := sr.FiveMinute.Sep >= -exitSep && sr.FiveMinute.EMAFastSlope >= 0
		return histExit || sepExit
	default:
		return false
	}
}

func (tm *tradeManager) startCooldown(symbol string) {
	tm.cooldownMu.Lock()
	if tm.cooldown == nil {
		tm.cooldown = make(map[string]int)
	}
	tm.cooldown[strings.ToUpper(symbol)] = 5
	tm.cooldownMu.Unlock()
}

func (tm *tradeManager) cooldownTick(symbol, interval string) {
	if strings.ToLower(interval) != "3m" {
		return
	}
	tm.cooldownMu.Lock()
	defer tm.cooldownMu.Unlock()
	symbol = strings.ToUpper(symbol)
	if symbol == "" {
		return
	}
	if tm.cooldown == nil {
		tm.cooldown = make(map[string]int)
	}
	if v, ok := tm.cooldown[symbol]; ok {
		if v <= 1 {
			delete(tm.cooldown, symbol)
		} else {
			tm.cooldown[symbol] = v - 1
		}
	}
}

func (tm *tradeManager) isCoolingDown(symbol string) bool {
	tm.cooldownMu.RLock()
	defer tm.cooldownMu.RUnlock()
	if tm.cooldown == nil {
		return false
	}
	return tm.cooldown[strings.ToUpper(symbol)] > 0
}

func (tm *tradeManager) OnCandleUpdate(symbol, interval string) {
	if tm == nil || !tm.cfg.autoTrade {
		return
	}
	symbol = strings.ToUpper(symbol)
	if symbol == "" {
		return
	}
	go tm.handleCandleUpdate(symbol, strings.ToLower(interval))
}

func (tm *tradeManager) handleCandleUpdate(symbol, interval string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sr, err := computeStrategyForSymbol(ctx, tm.watchMgr, symbol, tm.cfg)
	if err != nil {
		log.Printf("获取 %s 策略数据失败: %v", symbol, err)
	} else {
		tm.storeSnapshot(sr)
		if err := tm.checkPositionForSymbol(ctx, symbol, &sr); err != nil {
			log.Printf("基于3m更新检查持仓失败 %s: %v", symbol, err)
		}
		if err := tm.evaluateOpenForSymbol(ctx, symbol, &sr); err != nil {
			log.Printf("基于3m更新评估开仓失败 %s: %v", symbol, err)
		}
	}
	tm.cooldownTick(symbol, interval)
	tm.tickReentryCooldown(symbol, interval)
}

func (tm *tradeManager) checkPositionForSymbol(ctx context.Context, symbol string, snapshot *strategyResult) error {
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
	var sr strategyResult
	if snapshot != nil {
		sr = *snapshot
	} else if snap, ok := tm.getSnapshot(symbol); ok {
		sr = snap
	} else {
		var err error
		sr, err = computeStrategyForSymbol(ctx, tm.watchMgr, symbol, tm.cfg)
		if err != nil {
			return err
		}
		tm.storeSnapshot(sr)
	}
	var firstErr error
	if hasLong {
		if err := tm.handleExitForSide(ctx, symbol, "LONG", sr, true); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if hasShort {
		if err := tm.handleExitForSide(ctx, symbol, "SHORT", sr, true); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (tm *tradeManager) handleExitForSide(ctx context.Context, symbol, side string, sr strategyResult, allowBaseline bool) error {
	side = strings.ToUpper(side)
	if side != "LONG" && side != "SHORT" {
		return nil
	}
	if !tm.positions.Has(symbol, side) {
		return nil
	}
	tm.storeSnapshot(sr)
	var firstErr error
	sideLabel := "多"
	if side == "SHORT" {
		sideLabel = "空"
	}
	tm.updateTrailing(symbol, side, sr)
	if tm.isTightening(symbol, side) {
		if tm.shouldChandelierExit(symbol, side, sr) {
			if err := tm.closePosition(ctx, symbol, side); err != nil {
				log.Printf("吊灯平%s失败 %s: %v", sideLabel, symbol, err)
				tm.notifyError(ctx, fmt.Sprintf("平仓失败 %s %s: %v", symbol, side, err))
				if firstErr == nil {
					firstErr = err
				}
			} else {
				tm.clearTightening(symbol, side)
				tm.markReentryExit(symbol, side, sr.Metrics.Last, exitTypeHard)
			}
			return firstErr
		}
		closeConfirm := tm.shouldConfirmExit(symbol, side, sr)
		if closeConfirm && !tm.canSoftExit(symbol, side) {
			closeConfirm = false
		}
		if closeConfirm {
			if err := tm.closePosition(ctx, symbol, side); err != nil {
				log.Printf("平%s失败 %s: %v", sideLabel, symbol, err)
				tm.notifyError(ctx, fmt.Sprintf("平仓失败 %s %s: %v", symbol, side, err))
				if firstErr == nil {
					firstErr = err
				}
			} else {
				tm.clearTightening(symbol, side)
				tm.markReentryExit(symbol, side, sr.Metrics.Last, exitTypeSoft)
			}
			return firstErr
		}
		if shouldTightenPosition(side, sr) {
			tm.setTightening(symbol, side)
			tm.ensureTrailing(symbol, side, sr)
		} else {
			tm.clearTightening(symbol, side)
			tm.clearTrailing(symbol, side)
			log.Printf("%s %s 动量恢复，解除收紧", symbol, side)
		}
		return firstErr
	}
	if allowBaseline {
		closeBaseline := shouldBaselineExit(side, sr)
		if closeBaseline && !tm.canSoftExit(symbol, side) {
			closeBaseline = false
		}
		if closeBaseline {
			if err := tm.closePosition(ctx, symbol, side); err != nil {
				log.Printf("平%s失败 %s: %v", sideLabel, symbol, err)
				tm.notifyError(ctx, fmt.Sprintf("平仓失败 %s %s: %v", symbol, side, err))
				if firstErr == nil {
					firstErr = err
				}
			} else {
				tm.clearTightening(symbol, side)
				tm.markReentryExit(symbol, side, sr.Metrics.Last, exitTypeSoft)
			}
			return firstErr
		}
	}
	if shouldTightenPosition(side, sr) {
		if !tm.isTightening(symbol, side) {
			log.Printf("%s %s 触发动量预警，收紧止盈", symbol, side)
		}
		tm.setTightening(symbol, side)
		tm.ensureTrailing(symbol, side, sr)
	} else if tm.isTightening(symbol, side) {
		tm.clearTightening(symbol, side)
		tm.clearTrailing(symbol, side)
		log.Printf("%s %s 动量恢复，解除收紧", symbol, side)
	}
	return firstErr
}

func (tm *tradeManager) evaluateEntryEnvironment(ctx context.Context, sr *strategyResult) bool {
	if sr == nil {
		return true
	}
	sr.EntryBlocked = false
	sr.BlockReasons = nil
	sr.SpreadRatio = 0
	sr.DepthNotional = 0
	allowed := true
	if sr.FiveMinute.Choppiness > choppinessThreshold && sr.FiveMinute.ADX < choppinessAdxMax {
		reason := fmt.Sprintf("震荡过滤 Choppiness=%.1f ADX=%.1f", sr.FiveMinute.Choppiness, sr.FiveMinute.ADX)
		sr.BlockReasons = append(sr.BlockReasons, reason)
		allowed = false
	}
	if ok := tm.checkBTCCooloff(ctx); !ok {
		expiry := tm.getCooloffExpiry()
		reason := "BTC 15m 波动检测失败"
		if !expiry.IsZero() {
			reason = fmt.Sprintf("BTC 15m 波动冷静期至 %s", expiry.Format("15:04"))
		}
		sr.BlockReasons = append(sr.BlockReasons, reason)
		allowed = false
	}
	spreadAllowed := tm.assessSpreadAndDepth(ctx, sr)
	if !spreadAllowed {
		allowed = false
	}
	sr.EntryBlocked = !allowed
	return allowed
}

func (tm *tradeManager) assessSpreadAndDepth(ctx context.Context, sr *strategyResult) bool {
	if tm.client == nil || sr == nil {
		return true
	}
	atr := sr.ThreeMinute.ATR22
	if atr <= 0 {
		atr = sr.ThreeMinute.ATR50
	}
	bestBid, bestAsk, bidNotional, askNotional, err := tm.client.FetchDepth(ctx, sr.Symbol)
	if err != nil {
		log.Printf("获取 %s 深度失败: %v", sr.Symbol, err)
		sr.BlockReasons = append(sr.BlockReasons, "深度获取失败")
		return false
	}
	if bestBid <= 0 || bestAsk <= 0 || bestAsk <= bestBid {
		sr.BlockReasons = append(sr.BlockReasons, "报价异常")
		return false
	}
	spread := bestAsk - bestBid
	if atr > 0 {
		sr.SpreadRatio = spread / atr
	} else {
		sr.SpreadRatio = math.Inf(1)
	}
	depth := math.Min(bidNotional, askNotional)
	sr.DepthNotional = depth
	allowed := true
	if sr.SpreadRatio > spreadAtrThreshold {
		allowed = false
		sr.BlockReasons = append(sr.BlockReasons, fmt.Sprintf("点差占比 %.3f 超阈值 %.2f", sr.SpreadRatio, spreadAtrThreshold))
	}
	return allowed
}

func (tm *tradeManager) checkBTCCooloff(ctx context.Context) bool {
	if tm.httpClient == nil {
		return true
	}
	now := time.Now()
	tm.volMu.Lock()
	if now.Before(tm.btcCooloffUntil) {
		tm.volMu.Unlock()
		return false
	}
	if now.Sub(tm.lastBtcCheck) < time.Minute {
		tm.volMu.Unlock()
		return true
	}
	tm.lastBtcCheck = now
	tm.volMu.Unlock()
	candles, err := fetchHistoricalInterval(ctx, tm.httpClient, "BTCUSDT", "15m", 60)
	if err != nil {
		log.Printf("获取 BTC 15m 行情失败: %v", err)
		return false
	}
	clean := dedupeCandles(candles)
	if len(clean) < 52 {
		return true
	}
	last := clean[len(clean)-1]
	prev := clean[len(clean)-2]
	highLow := last.High - last.Low
	highClose := math.Abs(last.High - prev.Close)
	lowClose := math.Abs(last.Low - prev.Close)
	tr := math.Max(highLow, math.Max(highClose, lowClose))
	atrSeries, err := computeATRSeries(clean, 50)
	if err != nil || len(atrSeries) == 0 {
		return true
	}
	atr := atrSeries[len(atrSeries)-1]
	if atr <= 0 {
		return true
	}
	if tr <= btcVolMultiplier*atr {
		return true
	}
	expiry := time.Now().Add(btcCooloffDuration)
	tm.volMu.Lock()
	tm.btcCooloffUntil = expiry
	tm.volMu.Unlock()
	log.Printf("BTC 15m 波动触发冷静期，TR=%.4f ATR=%.4f，冷静期至 %s", tr, atr, expiry.Format(time.RFC3339))
	return false
}

func (tm *tradeManager) getCooloffExpiry() time.Time {
	tm.volMu.Lock()
	defer tm.volMu.Unlock()
	return tm.btcCooloffUntil
}

func (tm *tradeManager) evaluateOpenForSymbol(ctx context.Context, symbol string, snapshot *strategyResult) error {
	if tm == nil || tm.watchMgr == nil {
		return nil
	}
	if tm.positions.Remaining() <= 0 {
		return nil
	}
	if tm.isCoolingDown(symbol) {
		return nil
	}
	var sr strategyResult
	if snapshot != nil {
		sr = *snapshot
	} else if snap, ok := tm.getSnapshot(symbol); ok {
		sr = snap
	} else {
		var err error
		sr, err = computeStrategyForSymbol(ctx, tm.watchMgr, symbol, tm.cfg)
		if err != nil {
			return err
		}
		tm.storeSnapshot(sr)
	}
	tm.updateReentryReset(symbol, "LONG", sr)
	tm.updateReentryReset(symbol, "SHORT", sr)
	if sr.Score <= 0 {
		return nil
	}
	price := sr.Metrics.Last
	if price <= 0 {
		return nil
	}
	key := sr.Last3mBarID + "::" + sr.Last5mBarID
	if key != "::" && !tm.shouldEvaluateSymbolKey(symbol, key) {
		return nil
	}
	if !tm.evaluateEntryEnvironment(ctx, &sr) {
		if len(sr.BlockReasons) > 0 {
			log.Printf("%s 环境不满足开仓: %s", symbol, strings.Join(sr.BlockReasons, "; "))
		}
		return nil
	}
	var firstErr error
	if sr.LongSignal && !tm.positions.Has(symbol, "LONG") {
		if !tm.canEnterAfterReset(symbol, "LONG", sr) {
			if tm.isRecentlyClosed(symbol, "LONG") {
				sr.BlockReasons = append(sr.BlockReasons, "最近平仓冷却中")
			}
			goto shortCheck
		}
		if err := tm.openPosition(ctx, symbol, "LONG", price); err != nil {
			log.Printf("实时开多失败 %s: %v", symbol, err)
			tm.notifyError(ctx, fmt.Sprintf("开多失败 %s: %v", symbol, err))
			if firstErr == nil {
				firstErr = err
			}
		} else {
			tm.clearReentryState(symbol, "LONG")
		}
	}
shortCheck:
	if tm.positions.Remaining() <= 0 {
		return firstErr
	}
	if sr.ShortSignal && !tm.positions.Has(symbol, "SHORT") {
		if !tm.canEnterAfterReset(symbol, "SHORT", sr) {
			if tm.isRecentlyClosed(symbol, "SHORT") {
				sr.BlockReasons = append(sr.BlockReasons, "最近平仓冷却中")
			}
			return firstErr
		}
		if err := tm.openPosition(ctx, symbol, "SHORT", price); err != nil {
			log.Printf("实时开空失败 %s: %v", symbol, err)
			tm.notifyError(ctx, fmt.Sprintf("开空失败 %s: %v", symbol, err))
			if firstErr == nil {
				firstErr = err
			}
		} else {
			tm.clearReentryState(symbol, "SHORT")
		}
	}
	tm.storeSnapshot(sr)
	return firstErr
}

func computeStrategyForSymbol(ctx context.Context, watchMgr *symbolManager, symbol string, cfg config) (strategyResult, error) {
	symbol = strings.ToUpper(symbol)
	if symbol == "" {
		return strategyResult{}, errors.New("symbol 为空")
	}
	if watchMgr == nil {
		return strategyResult{}, errors.New("行情管理器未初始化")
	}
	w3 := watchMgr.getWatcher(symbol, "3m")
	w5 := watchMgr.getWatcher(symbol, "5m")
	if w3 == nil || w5 == nil {
		watchMgr.EnsureWatchers(ctx, []string{symbol})
		w3 = watchMgr.getWatcher(symbol, "3m")
		w5 = watchMgr.getWatcher(symbol, "5m")
		if w3 == nil || w5 == nil {
			return strategyResult{}, fmt.Errorf("未找到 %s 的行情缓存", symbol)
		}
	}
	candles3 := w3.snapshot()
	candles5 := w5.snapshot()
	if len(candles3) == 0 || len(candles5) == 0 {
		return strategyResult{}, errors.New("行情数据不足")
	}
	return computeStrategyFromCandles(symbol, candles3, candles5, cfg)
}

func (tm *tradeManager) refreshPositions(ctx context.Context) error {
	entries, err := tm.client.FetchOpenPositions(ctx)
	if err != nil {
		return err
	}
	tm.positions.Replace(entries)
	tm.syncEntryTimes(entries)
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
		tm.storeSnapshot(sr)
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
		if err := tm.handleExitForSide(ctx, symbol, side, sr, true); err != nil && firstErr == nil {
			firstErr = err
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

	for i := range strategies {
		sr := strategies[i]
		if sr.Score <= 0 {
			continue
		}
		tm.updateReentryReset(sr.Symbol, "LONG", sr)
		tm.updateReentryReset(sr.Symbol, "SHORT", sr)
		if tm.positions.Remaining() <= 0 {
			break
		}
		price := sr.Metrics.Last
		if price <= 0 {
			continue
		}
		key := sr.Last3mBarID + "::" + sr.Last5mBarID
		if key != "::" && !tm.shouldEvaluateSymbolKey(sr.Symbol, key) {
			if snap, ok := tm.getSnapshot(sr.Symbol); ok {
				sr.EntryBlocked = snap.EntryBlocked
				sr.BlockReasons = snap.BlockReasons
				sr.SpreadRatio = snap.SpreadRatio
				sr.DepthNotional = snap.DepthNotional
			}
			strategies[i] = sr
			continue
		}
		if !tm.evaluateEntryEnvironment(ctx, &sr) {
			strategies[i] = sr
			if len(sr.BlockReasons) > 0 {
				log.Printf("%s 环境不满足开仓: %s", sr.Symbol, strings.Join(sr.BlockReasons, "; "))
			}
			continue
		}
		tm.storeSnapshot(sr)
		if sr.LongSignal && !tm.positions.Has(sr.Symbol, "LONG") {
			if !tm.canEnterAfterReset(sr.Symbol, "LONG", sr) {
				if tm.isRecentlyClosed(sr.Symbol, "LONG") {
					sr.BlockReasons = append(sr.BlockReasons, "最近平仓冷却中")
				}
				strategies[i] = sr
				goto shortCheckLabel
			}
			if err := tm.openPosition(ctx, sr.Symbol, "LONG", price); err != nil {
				log.Printf("开多单失败 %s: %v", sr.Symbol, err)
				tm.notifyError(ctx, fmt.Sprintf("开多失败 %s: %v", sr.Symbol, err))
			} else {
				tm.clearReentryState(sr.Symbol, "LONG")
			}
		}
	shortCheckLabel:
		if tm.positions.Remaining() <= 0 {
			strategies[i] = sr
			break
		}
		if sr.ShortSignal && !tm.positions.Has(sr.Symbol, "SHORT") {
			if !tm.canEnterAfterReset(sr.Symbol, "SHORT", sr) {
				if tm.isRecentlyClosed(sr.Symbol, "SHORT") {
					sr.BlockReasons = append(sr.BlockReasons, "最近平仓冷却中")
				}
				strategies[i] = sr
				continue
			}
			if err := tm.openPosition(ctx, sr.Symbol, "SHORT", price); err != nil {
				log.Printf("开空单失败 %s: %v", sr.Symbol, err)
				tm.notifyError(ctx, fmt.Sprintf("开空失败 %s: %v", sr.Symbol, err))
			} else {
				tm.clearReentryState(sr.Symbol, "SHORT")
			}
		}
		strategies[i] = sr
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
	tm.setEntryTime(symbol, side, time.Now())
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
	margin := walletBalance / 3.0
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

func (tm *tradeManager) notifyError(_ context.Context, msg string) {
	if msg == "" {
		return
	}
	log.Printf("交易错误: %s", msg)
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
	tm.clearEntryTime(symbol, side)
	log.Printf("%s %s 平仓成功，数量 %s", symbol, side, formatQuantity(adjQty))
	return nil
}

func fetchHistoricalInterval(ctx context.Context, c *http.Client, symbol, interval string, limit int) ([]candle, error) {
	url := fmt.Sprintf("%s%s?symbol=%s&interval=%s&limit=%d", baseURL, klinesEP, symbol, interval, limit)
	var raw [][]interface{}
	if err := getJSON(ctx, c, url, &raw); err != nil {
		return nil, err
	}
	dur, err := time.ParseDuration(interval)
	if err != nil {
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
			CloseTime: openTime.Add(dur),
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
	concurrency           int
	updateInterval        time.Duration
	volumeRefresh         time.Duration
	emaFastPeriod         int
	emaSlowPeriod         int
	macdFastPeriod        int
	macdSlowPeriod        int
	macdSignalPeriod      int
	adxPeriod             int
	adxThreshold          float64
	emaDiffThreshold      float64
	adxDirectionThreshold float64
	autoTrade             bool
	orderQty              float64
	maxPositions          int
	leverage              int
	recvWindow            int
	apiKey                string
	apiSecret             string
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
	updateInterval := flag.Duration("update-interval", 10*time.Minute, "涨跌幅更新周期")
	volumeRefresh := flag.Duration("volume-refresh", 12*time.Hour, "成交量榜刷新周期")
	emaFast := flag.Int("ema-fast", 7, "5m EMA 快速周期")
	emaSlow := flag.Int("ema-slow", 25, "5m EMA 慢速周期")
	macdFast := flag.Int("macd-fast", 12, "MACD 快速 EMA 周期")
	macdSlow := flag.Int("macd-slow", 26, "MACD 慢速 EMA 周期")
	macdSignal := flag.Int("macd-signal", 9, "MACD 信号线 EMA 周期")
	adxPeriod := flag.Int("adx-period", 14, "5m ADX 周期")
	adxThreshold := flag.Float64("adx-threshold", 25, "ADX 趋势判断阈值")
	emaDiffThreshold := flag.Float64("ema-diff-threshold", 0.0, "5m EMA 快慢线差值阈值")
	adxDirectionThreshold := flag.Float64("adx-direction-threshold", 20.0, "5m ADX 最小值")
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
		concurrency:           *concurrency,
		updateInterval:        *updateInterval,
		volumeRefresh:         *volumeRefresh,
		emaFastPeriod:         *emaFast,
		emaSlowPeriod:         *emaSlow,
		macdFastPeriod:        *macdFast,
		macdSlowPeriod:        *macdSlow,
		macdSignalPeriod:      *macdSignal,
		adxPeriod:             *adxPeriod,
		adxThreshold:          *adxThreshold,
		emaDiffThreshold:      *emaDiffThreshold,
		adxDirectionThreshold: *adxDirectionThreshold,
		autoTrade:             *autoTrade,
		orderQty:              qty,
		maxPositions:          *maxPositions,
		leverage:              *leverage,
		recvWindow:            *recvWindow,
		apiKey:                os.Getenv("BINANCE_API_KEY"),
		apiSecret:             os.Getenv("BINANCE_API_SECRET"),
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	if cfg.updateInterval <= 0 {
		cfg.updateInterval = 10 * time.Minute
	}
	if cfg.volumeRefresh <= 0 {
		cfg.volumeRefresh = 12 * time.Hour
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
	if cfg.emaDiffThreshold < 0 {
		cfg.emaDiffThreshold = 0
	}
	if cfg.adxDirectionThreshold <= 0 {
		cfg.adxDirectionThreshold = 20
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

	computeAndEvaluate := func(parent context.Context) error {
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
		if cfg.autoTrade && tradeMgr != nil {
			tradeMgr.UpdateQuantitiesFromMetrics(ctxUpdate, results)
			tradeMgr.HandleSignals(ctxUpdate, strategies)
		}
		log.Printf("完成行情评估，共评估 %d 个交易对，生成策略: %d 条", len(watchSymbols), len(strategies))
		return nil
	}

	refreshPositionSnapshots := func(parent context.Context) error {
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
		log.Printf("已更新持仓快照，持仓数: %d", len(entries))
		return nil
	}

	if err := refreshVolumeList(ctx); err != nil {
		return err
	}

	if err := computeAndEvaluate(ctx); err != nil {
		log.Printf("首次行情评估失败: %v", err)
	}

	volumeTicker := time.NewTicker(cfg.volumeRefresh)
	defer volumeTicker.Stop()
	updateTicker := time.NewTicker(cfg.updateInterval)
	defer updateTicker.Stop()
	positionTicker := time.NewTicker(6 * time.Minute)
	defer positionTicker.Stop()

	log.Printf("启动完成：成交量刷新周期 %s，指标评估周期 %s", cfg.volumeRefresh, cfg.updateInterval)

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
			if err := computeAndEvaluate(ctx); err != nil {
				log.Printf("行情评估失败: %v", err)
			}
		case <-positionTicker.C:
			if err := refreshPositionSnapshots(ctx); err != nil {
				log.Printf("更新持仓快照失败: %v", err)
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
	macdHistPrevPrev := macdHistPrev
	if lastIdx >= 2 {
		macdPrevPrevLine := macdLine[lastIdx-2]
		signalPrevPrev := macdSignalSeries[lastIdx-2]
		macdHistPrevPrev = macdPrevPrevLine - signalPrevPrev
	}
	macdHistSeries := make([]float64, len(macdSignalSeries))
	for i := range macdHistSeries {
		macdHistSeries[i] = macdLine[i] - macdSignalSeries[i]
	}

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
	analysis.MACDPrevPrev = macdHistPrevPrev

	adxPrev := adxVal
	if len(clean) > cfg.adxPeriod+1 {
		if val, err := computeADX(clean[:len(clean)-1], cfg.adxPeriod); err == nil {
			adxPrev = val
		}
	}
	adxPrev2 := adxPrev
	if len(clean) > cfg.adxPeriod+2 {
		if val, err := computeADX(clean[:len(clean)-2], cfg.adxPeriod); err == nil {
			adxPrev2 = val
		}
	}
	adxPrev3 := adxPrev2
	if len(clean) > cfg.adxPeriod+3 {
		if val, err := computeADX(clean[:len(clean)-3], cfg.adxPeriod); err == nil {
			adxPrev3 = val
		}
	}
	analysis.ADX = adxVal
	analysis.ADXPrev = adxPrev
	analysis.ADXPrev2 = adxPrev2
	analysis.ADXPrev3 = adxPrev3
	analysis.MACDStd200 = stddevLastN(macdHistSeries, 200)
	analysis.MACDEpsilon = analysis.MACDStd200 * macdStdMultiplier
	if atrSeries, err := computeATRSeries(clean, 50); err == nil && len(atrSeries) > 0 {
		analysis.ATR50 = atrSeries[len(atrSeries)-1]
	}
	if analysis.ATR50 > 0 {
		analysis.Sep = (analysis.EMAFast - analysis.EMASlow) / analysis.ATR50
	}
	if atr22, err := computeATRSeries(clean, 22); err == nil && len(atr22) > 0 {
		analysis.ATR22 = atr22[len(atr22)-1]
	}
	if len(clean) >= 22 {
		window := clean[len(clean)-22:]
		high := window[0].High
		low := window[0].Low
		for _, c := range window {
			if c.High > high {
				high = c.High
			}
			if c.Low < low {
				low = c.Low
			}
		}
		analysis.HighestHigh22 = high
		analysis.LowestLow22 = low
	} else if len(clean) > 0 {
		high := clean[0].High
		low := clean[0].Low
		for _, c := range clean {
			if c.High > high {
				high = c.High
			}
			if c.Low < low {
				low = c.Low
			}
		}
		analysis.HighestHigh22 = high
		analysis.LowestLow22 = low
	}
	if chop, err := computeChoppiness(clean, 14); err == nil {
		analysis.Choppiness = chop
	}
	analysis.LongSignal = false
	analysis.ShortSignal = false
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

func computeStrategyFromCandles(symbol string, candles3m, candles5m []candle, cfg config) (strategyResult, error) {
	result := strategyResult{Symbol: symbol}
	clean3 := dedupeCandles(candles3m)
	clean5 := dedupeCandles(candles5m)
	if len(clean3) < 121 {
		return result, errors.New("3m K线不足 121 条")
	}
	if len(clean5) < 121 {
		return result, errors.New("5m K线不足 121 条")
	}

	metrics, err := computeMetricsFromCandles(symbol, clean3)
	if err != nil {
		return result, err
	}
	result.Metrics = metrics

	last3 := clean3[len(clean3)-1]
	last5 := clean5[len(clean5)-1]
	result.Last3mBarID = fmt.Sprintf("%d", last3.CloseTime.UnixNano())
	result.Last5mBarID = fmt.Sprintf("%d", last5.CloseTime.UnixNano())

	threeMinute, err := computeTimeframeAnalysis("3m", clean3, cfg)
	if err != nil {
		return result, err
	}

	fiveMinute, err := computeTimeframeAnalysis("5m", clean5, cfg)
	if err != nil {
		return result, err
	}

	result.ThreeMinute = threeMinute
	result.FiveMinute = fiveMinute

	emaDiffLong := fiveMinute.EMAFast - fiveMinute.EMASlow
	emaDiffShort := fiveMinute.EMASlow - fiveMinute.EMAFast
	adxThreshold := cfg.adxDirectionThreshold
	if adxThreshold < 22 {
		adxThreshold = 22
	}
	adxDelta3 := fiveMinute.ADX - fiveMinute.ADXPrev3
	adxRising := fiveMinute.ADX >= adxThreshold && adxDelta3 > 0
	enterSep := sepEnterThreshold
	sep := fiveMinute.Sep

	longDirection := adxRising && sep >= enterSep && emaDiffLong > cfg.emaDiffThreshold
	shortDirection := adxRising && sep <= -enterSep && emaDiffShort > cfg.emaDiffThreshold

	epsilon := threeMinute.MACDEpsilon
	macdCrossLong := threeMinute.MACDHist > epsilon && threeMinute.MACDPrev <= epsilon
	macdCrossShort := threeMinute.MACDHist < -epsilon && threeMinute.MACDPrev >= -epsilon

	emaBounceLong := false
	emaBounceShort := false
	if len(clean3) > 1 {
		prev := clean3[len(clean3)-2]
		last := clean3[len(clean3)-1]
		emaBounceLong = prev.Close <= threeMinute.EMAFast && last.Close > threeMinute.EMAFast
		emaBounceShort = prev.Close >= threeMinute.EMAFast && last.Close < threeMinute.EMAFast
	}

	timingLong := (macdCrossLong || emaBounceLong) && threeMinute.MACDHist > epsilon
	timingShort := (macdCrossShort || emaBounceShort) && threeMinute.MACDHist < -epsilon

	result.LongSignal = longDirection && timingLong
	result.ShortSignal = shortDirection && timingShort

	result.Score = 0
	if result.LongSignal {
		sepScore := sep - enterSep
		if sepScore < 0 {
			sepScore = 0
		}
		adxScore := fiveMinute.ADX - adxThreshold
		if adxScore < 0 {
			adxScore = 0
		}
		crossScore := 0.0
		if macdCrossLong {
			crossScore += math.Abs(threeMinute.MACDHist)
		}
		if emaBounceLong {
			crossScore += math.Abs(clean3[len(clean3)-1].Close - threeMinute.EMAFast)
		}
		result.Score = sepScore + adxScore + crossScore
	} else if result.ShortSignal {
		sepScore := (-enterSep) - sep
		if sepScore < 0 {
			sepScore = 0
		}
		adxScore := fiveMinute.ADX - adxThreshold
		if adxScore < 0 {
			adxScore = 0
		}
		crossScore := 0.0
		if macdCrossShort {
			crossScore += math.Abs(threeMinute.MACDHist)
		}
		if emaBounceShort {
			crossScore += math.Abs(clean3[len(clean3)-1].Close - threeMinute.EMAFast)
		}
		result.Score = sepScore + adxScore + crossScore
	}

	return result, nil
}

func fetchSymbolMetrics(ctx context.Context, c *http.Client, symbol string) (symbolMetrics, error) {
	metrics := symbolMetrics{Symbol: symbol}
	candles, err := fetchHistoricalInterval(ctx, c, symbol, "3m", positionKlineLimit)
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

func stddevLastN(values []float64, window int) float64 {
	if window <= 1 || len(values) < 2 {
		return 0
	}
	if len(values) < window {
		window = len(values)
	}
	segment := values[len(values)-window:]
	sum := 0.0
	for _, v := range segment {
		sum += v
	}
	mean := sum / float64(window)
	variance := 0.0
	for _, v := range segment {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(window)
	return math.Sqrt(variance)
}

func computeATRSeries(candles []candle, period int) ([]float64, error) {
	if period <= 0 {
		return nil, errors.New("ATR 周期需大于0")
	}
	if len(candles) < period {
		return nil, errors.New("K线数据不足以计算 ATR")
	}
	tr := make([]float64, len(candles))
	for i := range candles {
		if i == 0 {
			tr[i] = candles[i].High - candles[i].Low
			continue
		}
		highLow := candles[i].High - candles[i].Low
		highPrevClose := math.Abs(candles[i].High - candles[i-1].Close)
		lowPrevClose := math.Abs(candles[i].Low - candles[i-1].Close)
		tr[i] = math.Max(highLow, math.Max(highPrevClose, lowPrevClose))
	}
	atr := make([]float64, len(candles))
	sum := 0.0
	for i := 0; i < period && i < len(tr); i++ {
		sum += tr[i]
	}
	atr[period-1] = sum / float64(period)
	for i := period; i < len(tr); i++ {
		atr[i] = (atr[i-1]*(float64(period-1)) + tr[i]) / float64(period)
	}
	for i := 0; i < period-1 && i < len(atr); i++ {
		atr[i] = atr[period-1]
	}
	return atr, nil
}

func computeChoppiness(candles []candle, period int) (float64, error) {
	if period <= 0 {
		return 0, errors.New("Choppiness 周期需大于0")
	}
	if len(candles) < period+1 {
		return 0, errors.New("K线数据不足以计算 Choppiness")
	}
	sumTR := 0.0
	highestHigh := candles[len(candles)-period].High
	lowestLow := candles[len(candles)-period].Low
	for i := len(candles) - period; i < len(candles); i++ {
		c := candles[i]
		prevClose := candles[i-1].Close
		highLow := c.High - c.Low
		highClose := math.Abs(c.High - prevClose)
		lowClose := math.Abs(c.Low - prevClose)
		tr := math.Max(highLow, math.Max(highClose, lowClose))
		sumTR += tr
		if c.High > highestHigh {
			highestHigh = c.High
		}
		if c.Low < lowestLow {
			lowestLow = c.Low
		}
	}
	rangeSize := highestHigh - lowestLow
	if rangeSize <= 0 || sumTR <= 0 {
		return 0, errors.New("无法计算 Choppiness")
	}
	chop := math.Log10(sumTR/rangeSize) / math.Log10(float64(period))
	return chop * 100, nil
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
