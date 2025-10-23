package main

import (
	"bufio"
	"context"
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
	initialKlineLimit = 1500
	max1mCandles      = 2000
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

type strategyResult struct {
	Symbol       string
	Metrics      symbolMetrics
	EMAFast      float64
	EMASlow      float64
	EMAFastSlope float64
	EMASlowSlope float64
	MACDHist     float64
	MACDPrev     float64
	ADX          float64
	Passed       bool
}

type symbolWatcher struct {
	symbol  string
	mu      sync.RWMutex
	candles []candle
	cancel  context.CancelFunc
	done    chan struct{}
}

type symbolManager struct {
	mu       sync.RWMutex
	watchers map[string]*symbolWatcher
	client   *http.Client
	cfg      config
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
	}
}

func (m *symbolManager) EnsureWatchers(ctx context.Context, symbols []string) {
	for _, sym := range symbols {
		sym = strings.ToUpper(sym)
		if sym == "" {
			continue
		}
		m.mu.RLock()
		_, exists := m.watchers[sym]
		m.mu.RUnlock()
		if exists {
			continue
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
		candles, err := fetchHistorical1m(ctxTimeout, m.client, sym, initialKlineLimit)
		cancel()
		if err != nil {
			log.Printf("初始化 %s 监听失败: %v", sym, err)
			continue
		}

		watcher := newSymbolWatcher(sym, candles)

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

func newSymbolWatcher(symbol string, candles []candle) *symbolWatcher {
	copyCandles := make([]candle, len(candles))
	copy(copyCandles, candles)
	return &symbolWatcher{
		symbol:  symbol,
		candles: copyCandles,
		done:    make(chan struct{}),
	}
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
	defer w.mu.Unlock()
	n := len(w.candles)
	if n > 0 && w.candles[n-1].OpenTime.Equal(c.OpenTime) {
		w.candles[n-1] = c
		return
	}
	w.candles = append(w.candles, c)
	if len(w.candles) > max1mCandles {
		w.candles = w.candles[len(w.candles)-max1mCandles:]
	}
}

func (w *symbolWatcher) snapshot() []candle {
	w.mu.RLock()
	defer w.mu.RUnlock()
	copyCandles := make([]candle, len(w.candles))
	copy(copyCandles, w.candles)
	return copyCandles
}

func (w *symbolWatcher) Close() {
	if w.cancel != nil {
		w.cancel()
	}
	<-w.done
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
	return candles, nil
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
	concurrency    int
	top            int
	updateInterval time.Duration
	volumeRefresh  time.Duration
	telegramToken  string
	telegramChatID string
	watchCount     int
	emaFastPeriod  int
	emaSlowPeriod  int
	adxPeriod      int
	adxThreshold   float64
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
	top := flag.Int("top", 10, "输出涨跌幅排名数量")
	updateInterval := flag.Duration("update-interval", 10*time.Minute, "涨跌幅更新周期")
	volumeRefresh := flag.Duration("volume-refresh", 12*time.Hour, "成交量榜刷新周期")
	watchCount := flag.Int("watch-count", 20, "监听币种数量")
	emaFast := flag.Int("ema-fast", 12, "5m EMA 快速周期")
	emaSlow := flag.Int("ema-slow", 26, "5m EMA 慢速周期")
	adxPeriod := flag.Int("adx-period", 14, "5m ADX 周期")
	adxThreshold := flag.Float64("adx-threshold", 25, "ADX 趋势判断阈值")
	flag.Parse()

	cfg := config{
		concurrency:    *concurrency,
		top:            *top,
		updateInterval: *updateInterval,
		volumeRefresh:  *volumeRefresh,
		telegramToken:  os.Getenv("TELEGRAM_BOT_TOKEN"),
		telegramChatID: os.Getenv("TELEGRAM_CHAT_ID"),
		watchCount:     *watchCount,
		emaFastPeriod:  *emaFast,
		emaSlowPeriod:  *emaSlow,
		adxPeriod:      *adxPeriod,
		adxThreshold:   *adxThreshold,
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
		cfg.emaFastPeriod = 12
	}
	if cfg.emaSlowPeriod <= cfg.emaFastPeriod {
		cfg.emaSlowPeriod = cfg.emaFastPeriod + 10
	}
	if cfg.adxPeriod < 1 {
		cfg.adxPeriod = 14
	}
	if cfg.adxThreshold <= 0 {
		cfg.adxThreshold = 25
	}
	return cfg
}

func run(cfg config) error {
	if cfg.telegramToken == "" || cfg.telegramChatID == "" {
		return errors.New("未配置 Telegram 凭证：请在环境变量 TELEGRAM_BOT_TOKEN、TELEGRAM_CHAT_ID 中设置")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	httpClient := &http.Client{Timeout: 12 * time.Second}
	watchMgr := newSymbolManager(httpClient, cfg)
	defer watchMgr.Close()

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
		watchSymbols := selectTopSymbols(results, cfg.watchCount)
		if len(watchSymbols) > 0 {
			watchMgr.EnsureWatchers(ctx, watchSymbols)
		}
		strategies := watchMgr.EvaluateStrategies(watchSymbols)
		message := buildTelegramMessage(time.Now(), len(snapshot), cfg.top, gainers, losers, strategies, watchSymbols)
		if message == "" {
			return errors.New("推送内容为空")
		}

		if err := sendTelegramMessage(ctxUpdate, httpClient, cfg.telegramToken, cfg.telegramChatID, message); err != nil {
			return fmt.Errorf("发送Telegram消息失败: %w", err)
		}
		log.Printf("已推送Telegram通知，涨幅榜: %d 条，跌幅榜: %d 条，监听币种: %d", len(gainers), len(losers), len(watchSymbols))
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

	// 按绝对涨跌幅降序
	sort.Slice(res, func(i, j int) bool {
		ai := math.Abs(res[i].Change10m)
		aj := math.Abs(res[j].Change10m)
		if ai == aj {
			return res[i].Change10m > res[j].Change10m
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
		if r.Change10m > 0 {
			gainers = append(gainers, r)
		} else if r.Change10m < 0 {
			losers = append(losers, r)
		}
	}
	sort.Slice(gainers, func(i, j int) bool { return gainers[i].Change10m > gainers[j].Change10m })
	sort.Slice(losers, func(i, j int) bool { return losers[i].Change10m < losers[j].Change10m })
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

func buildTelegramMessage(now time.Time, volumeCount int, top int, gainers, losers []symbolMetrics, strategies []strategyResult, watchSymbols []string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("时间: %s\n", now.Format("2006-01-02 15:04:05")))
	b.WriteString(fmt.Sprintf("成交量前50%%交易对数: %d\n", volumeCount))
	if len(watchSymbols) > 0 {
		b.WriteString(fmt.Sprintf("当前监听币种(%d): %s\n", len(watchSymbols), strings.Join(watchSymbols, ", ")))
	}
	b.WriteString(fmt.Sprintf("10m 涨幅 Top%d:\n", top))
	if len(gainers) == 0 {
		b.WriteString("暂无涨幅数据\n")
	} else {
		if len(gainers) < top {
			b.WriteString(fmt.Sprintf("（仅 %d 个满足条件）\n", len(gainers)))
		}
		for i, g := range gainers {
			b.WriteString(fmt.Sprintf("%2d) %-12s 10m:%+0.4f%% 30m:%+0.4f%% 1h:%+0.4f%% 2h:%+0.4f%% Avg振幅:%0.4f%% 收盘价: %.8f\n",
				i+1,
				g.Symbol,
				g.Change10m,
				g.Change30m,
				g.Change60m,
				g.Change120m,
				g.AvgAmplitude,
				g.Last,
			))
		}
	}
	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("10m 跌幅 Top%d:\n", top))
	if len(losers) == 0 {
		b.WriteString("暂无跌幅数据\n")
	} else {
		if len(losers) < top {
			b.WriteString(fmt.Sprintf("（仅 %d 个满足条件）\n", len(losers)))
		}
		for i, l := range losers {
			b.WriteString(fmt.Sprintf("%2d) %-12s 10m:%+0.4f%% 30m:%+0.4f%% 1h:%+0.4f%% 2h:%+0.4f%% Avg振幅:%0.4f%% 收盘价: %.8f\n",
				i+1,
				l.Symbol,
				l.Change10m,
				l.Change30m,
				l.Change60m,
				l.Change120m,
				l.AvgAmplitude,
				l.Last,
			))
		}
	}
	b.WriteString("\n")
	b.WriteString("5m 策略监控 (EMA/MACD/ADX)：\n")
	if len(strategies) == 0 {
		b.WriteString("暂无监听数据或数据不足\n")
	} else {
		for _, sr := range strategies {
			status := "❌"
			if sr.Passed {
				status = "✅"
			}
			b.WriteString(fmt.Sprintf("%s %-10s 10m:%+0.4f%% Avg振幅:%0.4f%% EMA快:%0.4f(Δ%0.4f) EMA慢:%0.4f(Δ%0.4f) MACD_H:%0.4f→%0.4f ADX:%0.2f\n",
				status,
				sr.Symbol,
				sr.Metrics.Change10m,
				sr.Metrics.AvgAmplitude,
				sr.EMAFast,
				sr.EMAFastSlope,
				sr.EMASlow,
				sr.EMASlowSlope,
				sr.MACDHist,
				sr.MACDPrev,
				sr.ADX,
			))
		}
	}
	return strings.TrimSpace(b.String())
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

func computeStrategyFromCandles(symbol string, candles []candle, cfg config) (strategyResult, error) {
	result := strategyResult{Symbol: symbol}
	if len(candles) < 121 {
		return result, errors.New("1m K线不足 121 条")
	}
	closes := make([]float64, 0, len(candles))
	highs := make([]float64, 0, len(candles))
	lows := make([]float64, 0, len(candles))
	for _, cndl := range candles {
		closes = append(closes, cndl.Close)
		highs = append(highs, cndl.High)
		lows = append(lows, cndl.Low)
	}

	metrics, err := computeMetricsFromSeries(symbol, closes, highs, lows)
	if err != nil {
		return result, err
	}
	result.Metrics = metrics

	fiveMinute := aggregateTo5m(candles)
	if len(fiveMinute) < cfg.emaSlowPeriod+1 {
		return result, errors.New("5m K线不足以计算EMA")
	}
	closes5m := make([]float64, 0, len(fiveMinute))
	for _, cndl := range fiveMinute {
		closes5m = append(closes5m, cndl.Close)
	}

	emaFast := emaSeries(closes5m, cfg.emaFastPeriod)
	emaSlow := emaSeries(closes5m, cfg.emaSlowPeriod)
	if len(emaFast) == 0 || len(emaSlow) == 0 {
		return result, errors.New("EMA 数据不足")
	}
	lastIdx := len(closes5m) - 1
	if lastIdx < 1 {
		return result, errors.New("5m K线不足以获取斜率")
	}
	fastCurrent := emaFast[lastIdx]
	fastPrev := emaFast[lastIdx-1]
	slowCurrent := emaSlow[lastIdx]
	slowPrev := emaSlow[lastIdx-1]
	result.EMAFast = fastCurrent
	result.EMASlow = slowCurrent
	result.EMAFastSlope = fastCurrent - fastPrev
	result.EMASlowSlope = slowCurrent - slowPrev

	macdHist := fastCurrent - slowCurrent
	macdPrev := fastPrev - slowPrev
	result.MACDHist = macdHist
	result.MACDPrev = macdPrev

	adxVal, err := computeADX(fiveMinute, cfg.adxPeriod)
	if err != nil {
		return result, err
	}
	result.ADX = adxVal

	result.Passed = fastCurrent > slowCurrent && result.EMAFastSlope > 0 && result.EMASlowSlope > 0 && macdHist > 0 && macdHist > macdPrev && adxVal > cfg.adxThreshold
	return result, nil
}

func fetchSymbolMetrics(ctx context.Context, c *http.Client, symbol string) (symbolMetrics, error) {
	metrics := symbolMetrics{Symbol: symbol}
	url := fmt.Sprintf("%s%s?symbol=%s&interval=1m&limit=499", baseURL, klinesEP, symbol)
	var raw [][]interface{}
	if err := getJSON(ctx, c, url, &raw); err != nil {
		return metrics, err
	}
	if len(raw) < 121 {
		return metrics, errors.New("K线数据不足")
	}

	closes := make([]float64, 0, len(raw))
	highs := make([]float64, 0, len(raw))
	lows := make([]float64, 0, len(raw))
	for _, row := range raw {
		if len(row) < 6 {
			continue
		}
		closeVal := parseStringFloat(row[4])
		highVal := parseStringFloat(row[2])
		lowVal := parseStringFloat(row[3])
		if math.IsNaN(closeVal) || math.IsNaN(highVal) || math.IsNaN(lowVal) {
			continue
		}
		closes = append(closes, closeVal)
		highs = append(highs, highVal)
		lows = append(lows, lowVal)
	}

	return computeMetricsFromSeries(symbol, closes, highs, lows)
}

func computeMetricsFromSeries(symbol string, closes, highs, lows []float64) (symbolMetrics, error) {
	metrics := symbolMetrics{Symbol: symbol}
	if len(closes) < 121 || len(highs) < len(closes) || len(lows) < len(closes) {
		return metrics, errors.New("K线数据不足")
	}
	metrics.Last = closes[len(closes)-1]
	calcChange := func(minutes int) (float64, error) {
		idx := len(closes) - (minutes + 1)
		if idx < 0 || idx >= len(closes) {
			return 0, errors.New("数据不足")
		}
		prev := closes[idx]
		if prev == 0 {
			return 0, errors.New("前值为0")
		}
		return (metrics.Last - prev) / prev * 100.0, nil
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

func aggregateTo5m(candles []candle) []candle {
	if len(candles) == 0 {
		return nil
	}
	aggregated := make([]candle, 0, len(candles)/5+1)
	var current candle
	var bucket time.Time
	count := 0
	for _, c := range candles {
		b := c.OpenTime.Truncate(5 * time.Minute)
		if count == 0 || !b.Equal(bucket) {
			if count > 0 {
				aggregated = append(aggregated, current)
			}
			bucket = b
			current = candle{
				OpenTime:  b,
				CloseTime: b.Add(5 * time.Minute),
				Open:      c.Open,
				High:      c.High,
				Low:       c.Low,
				Close:     c.Close,
				Volume:    c.Volume,
			}
			count = 1
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
		count++
	}
	if count > 0 {
		aggregated = append(aggregated, current)
	}
	return aggregated
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
