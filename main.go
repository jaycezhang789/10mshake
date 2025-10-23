package main

import (
    "context"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "math"
    "net/http"
    "os"
    "sort"
    "strconv"
    "sync"
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
    concurrency int
    top         int
}

func main() {
    cfg := parseConfig()
    if err := run(cfg); err != nil {
        fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
        os.Exit(1)
    }
}

func parseConfig() config {
    concurrency := flag.Int("concurrency", 10, "并发请求数量")
    top := flag.Int("top", 10, "输出涨跌幅排名数量")
    flag.Parse()

    cfg := config{
        concurrency: *concurrency,
        top:         *top,
    }
    if cfg.concurrency < 1 {
        cfg.concurrency = 1
    }
    if cfg.top < 1 {
        cfg.top = 1
    }
    return cfg
}

func run(cfg config) error {
    httpClient := &http.Client{Timeout: 12 * time.Second}
    ctx := context.Background()

    // 1) 获取U本位永续合约符号
    allowed, err := fetchUSDTPerps(ctx, httpClient)
    if err != nil {
        return fmt.Errorf("获取U本位合约失败: %w", err)
    }
    if len(allowed) == 0 {
        return errors.New("未获取到任何U本位永续合约")
    }

    // 2) 获取24小时ticker数据（全部），按基础资产成交量筛选前50%
    all, err := fetchAllTickers(ctx, httpClient)
    if err != nil {
        return fmt.Errorf("获取24h ticker失败: %w", err)
    }
    filt := filterAndRankTopHalfByVolume(all, allowed)
    if len(filt) == 0 {
        return errors.New("筛选后无可用交易对")
    }

    // 3) 对筛选后的交易对并发获取1m K线（每个499条），计算最近10分钟涨跌幅
    results := computeTopChanges(ctx, httpClient, filt, cfg.concurrency)
    if len(results) == 0 {
        return errors.New("未能计算任何交易对的10分钟涨跌幅")
    }

    // 4) 取涨跌幅绝对值前10名并输出
    topN := cfg.top
    if len(results) < topN {
        topN = len(results)
    }

    now := time.Now().Format("2006-01-02 15:04:05")
    fmt.Printf("时间: %s\n", now)
    fmt.Printf("U本位永续合约数: %d, 成交量前50%%: %d\n", len(allowed), len(filt))
    fmt.Printf("最近10分钟涨跌幅前%d名（按绝对值排序）：\n", topN)
    for i := 0; i < topN; i++ {
        r := results[i]
        sign := ""
        if r.Change >= 0 {
            sign = "+"
        }
        fmt.Printf("%2d) %-12s 10m涨跌幅: %s%.4f%%  最新价: %.8f\n", i+1, r.Symbol, sign, r.Change, r.Last)
    }

    fmt.Println()
    fmt.Println("说明: 交易量筛选采用24小时基础资产成交量(volume)，然后基于1m K线计算最近10分钟涨跌幅。")
    return nil
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
