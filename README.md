# Momentum — A 股量化交易系统

全自动 A 股短周期动量策略系统。盘前→竞价→盘中→盘后全覆盖，全部通过 **Bark** 推送手机。

## 功能矩阵

| 时段 | 模块 | 输出 | 推送 |
|------|------|------|:---:|
| 08:30 | 盘前早报 | 7维开仓评分(±3)、4级建议、政策快讯 | 📱 |
| 09:25 | 竞价扫描 | 情绪判断、涨停板块主线(封单≥3确认) | 📱 |
| 09:40-14:40 | 持仓监控 | 7条退出规则(止盈/止损/MA5破位) | 📱 |
| 14:44 | 尾盘选股 | 动量扫描、套牢盘过滤、自适应止损 | 📱 |
| 16:00 | 盘后归档 | 涨停/龙虎榜/机构动向/选股跟踪(D0-D3) | 📱 |
| 周五 | 周度回测 | 65天多周期回测 | 📱 |

## 核心参数

| 参数 | 值 | 说明 |
|------|-----|------|
| MAX_TOTAL_PICKS | 1 | 单次选股上限 |
| ENABLE_TRAPPED_FILTER | True | 套牢盘过滤 <10% |
| MAX_TRAPPED_RATIO | 0.10 | 套牢盘阈值 |

## 回测退出规则

| 优先级 | 规则 | 条件 |
|--------|------|------|
| 1 | 固定止盈 | 盘中高 ≥ 买入×1.10 |
| 2 | 固定止损 | 盘中低 ≤ 买入×0.95 |
| 3 | MA5 | 收盘 < MA5 |
| 4 | 乖离 | Bias ≥ 20% |
| 5 | RSI | RSI ≥ 80 |
| 6 | MA20 | 收盘 < MA20 |
| 7 | 到期 | 持仓 5 天 |

实验性 ATR 退出：ATR追踪(2.2×)、时间止损(2d+<1.8%)、BIAS脉冲(5d>4.5%)

## 6 个自动化工作流

所有工作流通过 `BARK_DEVICE_KEY` (GitHub Secret) 推送手机。
`actions/cache@v4` 持久化 `qlib_pro_v16.db` 跨天复用。

| 工作流 | 触发 | 时间 |
|--------|------|------|
| pre-market.yml | GitHub cron | 08:30 |
| auction-scan.yml | cron-job.org | 09:25 |
| position-monitor.yml | GitHub cron | 09:40-14:40 |
| momentum-scan.yml | cron-job.org | 14:25→14:44 |
| eod-analysis.yml | GitHub cron | 16:00 |
| momentum-backtest.yml | GitHub cron | 周五 15:05 |

## 外部 Cron (cron-job.org)

尾盘: `.../momentum-scan.yml/dispatches` POST `{"ref":"main","inputs":{"run_mode":"full"}}` Cron `25 14 * * 1-5` Asia/Shanghai

竞价: `.../auction-scan.yml/dispatches` POST `{"ref":"main"}` Cron `25 1 * * 1-5` Asia/Shanghai

## 通知配置 (Bark)

1. App Store 下载 Bark → 获取 Device Key
2. GitHub Secrets → `BARK_DEVICE_KEY` = `https://api.day.app/<key>/`

## 本地

```bash
python main.py --mode scan
python main.py --mode backtest --days 65 --periods 5 --no-report
MOMENTUM_USE_30M_EXIT=true python main.py --mode backtest --days 65 --periods 5 --no-report
```

## FAQ

| 问题 | 原因 | 解决 |
|------|------|------|
| 回测 0 交易 | 套牢盘过滤 / API 故障 | 降级为 stock 推导交易日 |
| 盘后无数据 | 实时 API 收盘失效 | K 线缓存降级 |
| Bark 收不到 | device key 未配 | 检查 Secret |
