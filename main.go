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
)

const (
	baseURL        = "https://fapi.binance.com"
	exchangeInfoEP = "/fapi/v1/exchangeInfo"
	ticker24hrEP   = "/fapi/v1/ticker/24hr"
	klinesEP       = "/fapi/v1/klines"
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

type symbolChange struct {
	Symbol string
	Change float64 // 10分钟涨跌幅（百分比）
	Last   float64 // 最新收盘价
}

type config struct {
	concurrency    int
	top            int
	updateInterval time.Duration
	volumeRefresh  time.Duration
	telegramToken  string
	telegramChatID string
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
	flag.Parse()

	cfg := config{
		concurrency:    *concurrency,
		top:            *top,
		updateInterval: *updateInterval,
		volumeRefresh:  *volumeRefresh,
		telegramToken:  os.Getenv("TELEGRAM_BOT_TOKEN"),
		telegramChatID: os.Getenv("TELEGRAM_CHAT_ID"),
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
	return cfg
}

func run(cfg config) error {
	if cfg.telegramToken == "" || cfg.telegramChatID == "" {
		return errors.New("未配置 Telegram 凭证：请在环境变量 TELEGRAM_BOT_TOKEN、TELEGRAM_CHAT_ID 中设置")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	httpClient := &http.Client{Timeout: 12 * time.Second}

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

		results := computeTopChanges(ctxUpdate, httpClient, snapshot, cfg.concurrency)
		if len(results) == 0 {
			return errors.New("未能计算任何交易对的10分钟涨跌幅")
		}

		gainers, losers := splitTopMovers(results, cfg.top)
		message := buildTelegramMessage(time.Now(), len(snapshot), cfg.top, gainers, losers)
		if message == "" {
			return errors.New("推送内容为空")
		}

		if err := sendTelegramMessage(ctxUpdate, httpClient, cfg.telegramToken, cfg.telegramChatID, message); err != nil {
			return fmt.Errorf("发送Telegram消息失败: %w", err)
		}
		log.Printf("已推送Telegram通知，涨幅榜: %d 条，跌幅榜: %d 条", len(gainers), len(losers))
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

func computeTopChanges(ctx context.Context, c *http.Client, symbols []string, concurrency int) []symbolChange {
	if concurrency < 1 {
		concurrency = 1
	}
	// 控制并发，尽量温和调用，避免触发限频
	sem := make(chan struct{}, concurrency)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	res := make([]symbolChange, 0, len(symbols))

	for _, s := range symbols {
		s := s
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			change, last, err := tenMinuteChange(ctx, c, s)
			if err != nil {
				return
			}
			mu.Lock()
			res = append(res, symbolChange{Symbol: s, Change: change, Last: last})
			mu.Unlock()
		}()
	}
	wg.Wait()

	// 按绝对涨跌幅降序
	sort.Slice(res, func(i, j int) bool {
		ai := math.Abs(res[i].Change)
		aj := math.Abs(res[j].Change)
		if ai == aj {
			return res[i].Change > res[j].Change
		}
		return ai > aj
	})
	return res
}

func splitTopMovers(results []symbolChange, top int) (gainers []symbolChange, losers []symbolChange) {
	if top < 1 {
		top = 1
	}
	for _, r := range results {
		if r.Change > 0 {
			gainers = append(gainers, r)
		} else if r.Change < 0 {
			losers = append(losers, r)
		}
	}
	sort.Slice(gainers, func(i, j int) bool { return gainers[i].Change > gainers[j].Change })
	sort.Slice(losers, func(i, j int) bool { return losers[i].Change < losers[j].Change })
	if len(gainers) > top {
		gainers = gainers[:top]
	}
	if len(losers) > top {
		losers = losers[:top]
	}
	return gainers, losers
}

func buildTelegramMessage(now time.Time, volumeCount int, top int, gainers, losers []symbolChange) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("时间: %s\n", now.Format("2006-01-02 15:04:05")))
	b.WriteString(fmt.Sprintf("成交量前50%%交易对数: %d\n", volumeCount))
	b.WriteString(fmt.Sprintf("10m 涨幅 Top%d:\n", top))
	if len(gainers) == 0 {
		b.WriteString("暂无涨幅数据\n")
	} else {
		if len(gainers) < top {
			b.WriteString(fmt.Sprintf("（仅 %d 个满足条件）\n", len(gainers)))
		}
		for i, g := range gainers {
			b.WriteString(fmt.Sprintf("%2d) %-12s %+0.4f%%  收盘价: %.8f\n", i+1, g.Symbol, g.Change, g.Last))
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
			b.WriteString(fmt.Sprintf("%2d) %-12s %+0.4f%%  收盘价: %.8f\n", i+1, l.Symbol, l.Change, l.Last))
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

func tenMinuteChange(ctx context.Context, c *http.Client, symbol string) (pct float64, last float64, err error) {
	url := fmt.Sprintf("%s%s?symbol=%s&interval=1m&limit=499", baseURL, klinesEP, symbol)
	var raw [][]interface{}
	if err := getJSON(ctx, c, url, &raw); err != nil {
		return 0, 0, err
	}
	if len(raw) < 12 { // 需要至少11根K线（10分钟前 + 最新）
		return 0, 0, errors.New("K线数据不足")
	}
	closes := make([]float64, 0, len(raw))
	for _, row := range raw {
		if len(row) < 6 {
			continue
		}
		// 收盘价在索引4，类型为字符串
		closeStr, ok := row[4].(string)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(closeStr, 64)
		if err != nil {
			continue
		}
		closes = append(closes, v)
	}
	if len(closes) < 12 {
		return 0, 0, errors.New("有效收盘价不足")
	}
	last = closes[len(closes)-1]
	prev := closes[len(closes)-11] // 10分钟前收盘
	if prev == 0 {
		return 0, last, errors.New("前值为0")
	}
	pct = (last - prev) / prev * 100.0
	return pct, last, nil
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
