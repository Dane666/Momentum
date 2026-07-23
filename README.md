# Momentum — A 股量化交易系统

全自动 A 股短周期量化策略系统。盘前→竞价→盘中→盘后全覆盖，策略回测与样本外验证全自动跑在 GitHub Actions，全部通过 **Bark** 推送手机。

## 功能矩阵

| 时段 | 模块 | 输出 | 推送 |
|------|------|------|:---:|
| 08:30 | 盘前早报 | 7维开仓评分(±3)、4级建议、政策快讯 | 📱 |
| 09:25 | 竞价扫描 | 情绪判断、涨停板块主线(封单≥3确认)、碳酸锂期货/韩股早盘辅助 | 📱 |
| 09:40-14:40 | 持仓监控 | 7条退出规则(止盈/止损/MA5破位) | 📱 |
| 14:44 | 尾盘选股 | 动量扫描、套牢盘过滤、自适应止损 | 📱 |
| 14:45 | 低位绩优股 | 深度超跌+绩优筛选(复用超跌绩优策略)、Bark推送 | 📱 |
| 16:00 | 盘后归档 | 涨停/龙虎榜/机构动向/选股跟踪(D0-D3) | 📱 |
| 周五 | 周度回测 | 65天多周期回测 | 📱 |

## 核心参数

| 参数 | 值 | 说明 |
|------|-----|------|
| MAX_TOTAL_PICKS | 1 | 单次选股上限 |
| ENABLE_TRAPPED_FILTER | True | 套牢盘过滤 <10% |
| MAX_TRAPPED_RATIO | 0.10 | 套牢盘阈值 |
| 实时数据源 | Sina → efinance → K线缓存 | 三级降级，任一级可用即正常、全失效才降级标记 |
| 竞价外部盘辅助 | 碳酸锂期货主连(eastmoney) + 韩股三星/海力士(yfinance) | 辅助判断锂矿股 / 存储-HBM 链开盘强弱 |

## 超跌绩优反弹策略

尾盘 14:45 的「低位绩优股」筛选与 `momentum-backtest.yml` 中的前向验证共用同一套 **超跌绩优反弹** 口径：筛选「深度超跌 + 绩优 + 热门」标的，并尽量在头部题材集中、单题材持仓上限 1。

**已发布最优组合（样本内选参）**
- 深度超跌(`base_signal`) + 绩优(`quality_ok`) + 热门板块
- 单题材持仓上限 = 1，止损 = -15%，无大盘择时
- 大盘择时对超跌反弹有害（弱市更易爆发行情），故有意不叠加择时

**回测表现**

| 区间 | 总收益 | 笔数 n | 胜率 | 夏普 | 最大回撤 |
|------|-------:|-----:|-----:|-----:|------:|
| 样本内（全窗口） | +23.61% | 10 | 70% | 1.011 | -10.76% |
| 样本外 TEST（OOS，本地） | +6.83% | 6 | 66.7% | 0.655 | -10.77% |
| 样本外 TEST（OOS，CI 近期） | +3.95% | 6 | 66.7% | 0.426 | -11.36% |

> 样本外盈利约为样本内的 29%，样本内显著高估，结论须保守。CI 与本地差异源于历史 K 线随数据商刷新/复权微调（已通过 #2 消除漂移，见下）。

**前向验证（Forward Validation）流程**
- 脚本：`opt_study/forward_validation.py`（复用 `harness_oversold_quality`，导入时覆盖绝对路径解耦）
- `auto` 模式：仅当样本外窗口 **≥20 个交易日** 才走真·前瞻（forward），否则退化为 holdout，避免仅多几天数据就跑出误导性的 n=0 结果
- 每月 1 日 16:00（含在 `momentum-backtest.yml`）自动运行；真·前瞻待 DB 累积满 20 个交易日（约 2026-08 中旬）后切换
- 产物：`forward_validation_report.html` / `_trades.csv` / `_metrics.json`（含 `mode`、`coverage_universe`、`auto_note`、`forward_window_days` 便于核查）

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

## 自动化工作流

所有工作流通过 `BARK_DEVICE_KEY` (GitHub Secret) 推送手机。
`actions/cache@v4` 持久化 `qlib_pro_v16.db` 跨天复用；验证 job 使用专用缓存键 `state-validation`，与 backtest job 的 `state-v1` 隔离，避免缓存碰撞。

| 工作流 | 触发 | 时间 |
|--------|------|------|
| pre-market.yml | GitHub cron | 08:30 |
| auction-scan.yml | cron-job.org | 09:25 |
| position-monitor.yml | GitHub cron | 09:40-14:40 |
| momentum-scan.yml | cron-job.org | 14:25→14:45 (含低位绩优股) |
| eod-analysis.yml | GitHub cron | 16:00 |
| momentum-backtest.yml | GitHub cron | 周五 15:05(动量回测) + 每月1日 16:00(含超跌绩优前向验证) |
| add-manual-position.yml | repository_dispatch | 手动录入持仓（手机 Webhook） |

## CI 健壮性优化

为保证生产回测/验证与本地一致、不被假绿掩盖，已落地以下加固：

| 项 | 措施 |
|----|------|
| #2 全量 DB 进 CI | 新建 GitHub Release `db-baseline-v1`（~220MB 全市场基准库：完整历史 K 线 + 基本面 + 板块缓存）。验证 job 仅当缓存未命中/库 <100MB 时下载，消除生产 vs 本地历史价漂移。`*.db` 仍 gitignored，不进 git。 |
| #4 数据门禁 | `forward_validation.py` 在 `coverage_universe < 1500`（或 DB 缺失）时 `sys.exit(1)` 判 FAIL，1500~4000 仅 WARN，杜绝预热不足/库损坏导致的假绿。 |
| #5 backtest 加速 | 回测 job 预预热由 `[:200]` 扩至**全市场非 ST A 股**（回测期 `load_or_fetch_kline` 读缓存，原仅预热 200 只致其余逐只网络抓取超时）；加 `continue-on-error`，超时 45→60min。 |
| auto 门槛 | `auto` 模式最小样本外窗口门槛 `MIN_OOS_TRADING_DAYS=20`，薄样本自动退化为 holdout。 |
| 专用缓存 key | 验证 job 缓存键由共用 `state-v1` 改为 `state-validation`，消除非确定性恢复（空缓存覆盖满缓存）。 |

## 因子研究

**KDJ 底部金叉（不采纳）**
- 回溯 2024-07~2026-06 共 **6200 笔**低位绩优候选日，比对「KDJ 底部金叉(D<30)」与「无金叉」的 N 日收益
- 20 日（实际持有期）胜率：金叉组 62.9% vs 无金叉组 63.6%；均值收益 2.76% vs 2.94%
- Welch t 检验 p=0.74 → 无显著差异，且方向在各周期不一致
- 结论：低位绩优候选本身已深度超卖，KDJ 几乎必然在超卖区，金叉是近乎冗余的同步信号——砍候选池却无质量提升，故**不采纳为因子**

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

# 超跌绩优前向验证（复用 harness 真实口径）
python opt_study/forward_validation.py validate --mode auto        # 自动判定 forward/holdout
python opt_study/forward_validation.py watchlist --lookback 10     # 当前候选观察池
python opt_study/kdj_factor_study.py                               # 因子回溯（需 numpy/pandas/scipy）
```

## FAQ

| 问题 | 原因 | 解决 |
|------|------|------|
| 回测 0 交易 | 套牢盘过滤 / API 故障 | 降级为 stock 推导交易日 |
| 盘后无数据 | 实时 API 收盘失效 | K 线缓存降级 |
| Bark 收不到 | device key 未配 | 检查 Secret |
| 验证 job 假绿 | 预热不足/库损坏 | #4 门禁已判 FAIL；检查 `coverage_universe` |
| CI 与本地收益差 | 历史价漂移 | #2 已用 Release 基准库对齐；真·前瞻待数据累积 |
