# 10mshake

Go 程序，用于监控币安 U 本位永续合约，定期筛选出成交量前 50% 的交易对，并将 30 分钟涨跌榜推送到 Telegram。

## 功能

- 每 12 小时重新获取一次所有 USDT 永续合约的 24h 基础资产成交量，并筛选成交量前 50% 的交易对。
- 每 10 分钟拉取这些交易对的 1m K 线（499 条），计算最近 30 分钟涨跌幅。
- 分别统计最近 30 分钟涨幅、跌幅前 N 名（默认 20），发送文本消息到 Telegram 指定聊天。
- 对涨跌榜排序后的前 `watch-count` 个交易对建立 1m WebSocket 监听，首次拉取 1500 条 1m K 线并持续更新，并在推送中展示每个交易对的实时潜在下单数量（已按最小步长取整）。
- 若启用自动下单，每 6 分钟额外推送账户当前持仓及 5m EMA/MACD/ADX 等平仓决策所需指标。
- 基于监听到的 1m 数据合成 5m K 线，额外计算 30 分钟、1 小时、2 小时涨跌幅、平均振幅，以及 5m 周期的 EMA/MACD/ADX 指标，并在推送中展示策略是否满足：
  - EMA 发散：EMA(fast) > EMA(slow) 且二者斜率向上
  - MACD 动量：MACD_H > 0 且柱状图放大
  - ADX 强度：ADX > `adx-threshold`
- 自动加载 `.env` 文件（若存在），便于管理私密凭证。

## 运行

要求本机已安装 Go 1.21+。程序启动后会持续运行，直到手动终止：

```bash
go run .
# 或构建二进制运行
go build -o 10mshake
./10mshake
```

## 命令行参数

- `-concurrency` 并发请求数量，默认 10。
- `-top` 涨跌榜数量（涨、跌各 N 名），默认 20。
- `-update-interval` 涨跌榜推送周期，默认 `10m`。
- `-volume-refresh` 成交量榜刷新周期，默认 `12h`。
- `-watch-count` 需要建立 WebSocket 监听的交易对数量，默认 20。
- `-ema-fast`、`-ema-slow` 5m EMA 快慢线周期，默认 7/25。
- `-macd-fast`、`-macd-slow`、`-macd-signal` MACD 快线、慢线与信号线周期，默认 12/26/9。
- `-adx-period` 5m ADX 计算周期，默认 14。
- `-adx-threshold` ADX 趋势强度阈值，默认 25。
- `-auto-trade` 启用自动下单逻辑，默认开启（如需仅观察行情，可传 `-auto-trade=false`）。
- `-order-qty` 每笔下单数量（或设置 `BINANCE_ORDER_QTY` 环境变量），默认 0（自动以可用余额的 1/10 作为保证金，并乘以杠杆换算名义价值）。
- `-max-positions` 最大持仓数量，默认 10。
- `-leverage` 杠杆倍数，默认 5。
- `-recv-window` Binance API 请求 recvWindow，默认 5000ms。

示例：

```bash
go run . -concurrency 8 -top 15 -update-interval 5m -volume-refresh 6h
```

## 环境变量 / `.env`

程序启动时会尝试加载当前目录下的 `.env` 文件（不会覆盖已存在的系统环境变量）。使用步骤：

1. 复制 `.env.example` 为 `.env`
2. 根据实际情况填写：
   - `TELEGRAM_BOT_TOKEN` Telegram 机器人 Token（必填）
   - `TELEGRAM_CHAT_ID` Telegram 接收方 Chat ID（必填）
   - `BINANCE_API_KEY`、`BINANCE_API_SECRET` 如需调用受限接口可填写（当前逻辑未使用，可留空）

## 说明

- 程序使用币安公开 REST API，无需 API Key 即可运行。
- 对 HTTP 请求增加了简单的重试与并发限制以减少触发限频的风险。
- Telegram 推送基于 `sendMessage` 接口，如需富文本可自行扩展格式化逻辑。
- 平均振幅按最近 10 条 1m K 线的 `(high - low) / low` 计算，去掉 2 个最大值和最小值后求均值。
- WebSocket 监听默认保留最近 1500 条 1m K 线，并基于这些数据合成 5m K 线与指标。
- 代码已使用 `gofmt` 整理；后续如需格式化，可执行 `/usr/local/go/bin/gofmt -w main.go`。

## 自动下单说明

- 通过 `-auto-trade` 开启真实交易；需在 `.env` 或环境变量中配置 `BINANCE_API_KEY`、`BINANCE_API_SECRET`。
- 每笔下单数量可通过 `-order-qty` 或 `BINANCE_ORDER_QTY` 指定；若留空则自动取当前可用余额的 1/10 作为保证金，结合杠杆换算名义价值，再除以最新价得到合约张数（同时在行情推送中展示候选数量）。程序默认使用全仓、双向持仓，并自动设置 5 倍杠杆（可调）。
- 依据策略：
  - 开多：`EMA_fast > EMA_slow` 且快慢线斜率向上，`MACD_H>0` 且放大（较上一棒增加），`ADX>阈值`。
  - 平多：`EMA_fast <= EMA_slow` 或 `MACD_H <= 0`。
  - 开空：条件同开多但方向相反。
  - 平空：`EMA_fast >= EMA_slow` 或 `MACD_H >= 0`。
- 最大持仓默认为 10；超过即暂停加仓。每次下单前会再次检查剩余可用仓位。
