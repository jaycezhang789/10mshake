# 10mshake

Go 程序，用于监控币安 U 本位永续合约，定期筛选出成交量前 50% 的交易对，并将 10 分钟涨跌榜推送到 Telegram。

## 功能

- 每 12 小时重新获取一次所有 USDT 永续合约的 24h 基础资产成交量，并筛选成交量前 50% 的交易对。
- 每 10 分钟拉取这些交易对的 1m K 线（499 条），计算最近 10 分钟涨跌幅。
- 推送消息中同时展示 30 分钟、1 小时、2 小时的涨跌幅，并统计最近 10 分钟的平均每分钟振幅（剔除 2 个最大值与最小值）。
- 分别统计最近 10 分钟涨幅、跌幅前 N 名（默认 10），发送文本消息到 Telegram 指定聊天。
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
- `-top` 涨跌榜数量（涨、跌各 N 名），默认 10。
- `-update-interval` 涨跌榜推送周期，默认 `10m`。
- `-volume-refresh` 成交量榜刷新周期，默认 `12h`。

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
- 代码已使用 `gofmt` 整理；后续如需格式化，可执行 `/usr/local/go/bin/gofmt -w main.go`。
