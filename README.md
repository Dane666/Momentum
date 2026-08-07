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
| 竞价外部盘辅助 | 碳酸锂期货主连(eastmoney) + 韩股三星/海力士(yfinance) | 辅助判断锂矿股 / 存储-HBM 链开盘强弱；相关 A 股「今开 vs 昨收」高开正反馈联动（✅确认 / ⚠️背离）；**正反馈阈值(高开±0.5% / 外盘±1%)与板块联动强度评分均在 `tools/auction_extra_config.json` 的 `feedback` 段可调** |

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
| volume-price-scan.yml | cron-job.org | 15:30 盘后价量计划池（突破放量 / 缩量回踩） |
| daily_inference.yml | **仅手动** `workflow_dispatch` | LightGBM 模型推荐（观察期，不定时推送） |

> 📚 文档索引见 [`docs/README.md`](docs/README.md#文档总览)。

## CI 健壮性优化

为保证生产回测/验证与本地一致、不被假绿掩盖，已落地以下加固：

| 项 | 措施 |
|----|------|
| #2 全量 DB 进 CI | 新建 GitHub Release `db-baseline-v1`（~220MB 全市场基准库：完整历史 K 线 + 基本面 + 板块缓存）。验证 job 仅当缓存未命中/库 <100MB 时下载，消除生产 vs 本地历史价漂移。`*.db` 仍 gitignored，不进 git。 |
| #4 数据门禁 | `forward_validation.py` 在 `coverage_universe < 1500`（或 DB 缺失）时 `sys.exit(1)` 判 FAIL，1500~4000 仅 WARN，杜绝预热不足/库损坏导致的假绿。 |
| #5 backtest 加速 | 回测 job 预预热由 `[:200]` 扩至**全市场非 ST A 股**（回测期 `load_or_fetch_kline` 读缓存，原仅预热 200 只致其余逐只网络抓取超时）；加 `continue-on-error`，超时 45→60min。 |
| auto 门槛 | `auto` 模式最小样本外窗口门槛 `MIN_OOS_TRADING_DAYS=20`，薄样本自动退化为 holdout。 |
| 专用缓存 key | 验证 job 缓存键由共用 `state-v1` 改为 `state-validation`，消除非确定性恢复（空缓存覆盖满缓存）。 |

## 统一监控与选股登记

所有策略（低位绩优 / C 尾盘 / 龙头 / 手动持仓）选出的股票，都通过**统一公共方法**
`momentum.tools.tracking_utils.add_picks()` 登记到 `data/picks_tracking.json` +
`stock_picks` 表，再由 `tools/position_monitor.py`（交易时段每 30 分钟）统一监控
止盈/止损触发并 Bark 推送。新增策略无需各自造一份保存逻辑，只需：

```python
from momentum.tools.tracking_utils import add_picks
add_picks(picks, 'MY_STRATEGY', sl_ratio=0.92, tp_ratio=1.12)   # 自动算止损/止盈、去重、同步 DB
```

- `add_picks` 按 `date + code + type` 去重，失败不影响主流程；`bark_notify()` 为统一推送入口
- `position_monitor` 触发时按类型中文标签推送：**低位绩优 / C 尾盘 / 龙头 / 策略 / 手动**
- 盘后 `tools/stock_picks_tracker.py`（16:00）产出 D0-D3 跟踪日报，含各类型图标
- 低位绩优筛选已剔除 ST / *ST / 退 / 仙股（<1.5 元），避免风险警示股混入绩优池

> **每日扫描 ≠ 回测**：14:45「低位绩优股」是**每日快照式筛选**（列出当天所有满足
> "深度超跌 + 绩优" 的标的，取评分 Top-N），而回测是**交易级回合模拟**（入场→持有→
> 退出，计 1 笔完整交易，含同股冷却 / 单题材上限）。二者口径不同——实盘每天 1~N 只
> 推荐、回测却约 1 笔/月，**属正常现象，不矛盾**。某只股票连续多天上榜 = 它连续多天
> 仍满足筛选条件（仍在超跌区），并非自动"更值得买"；需结合是否真正止跌反弹
> （RSI 拐头、不再创新低）判断，谨防"越跌越买"的价值陷阱。

## 价量口诀 · 盘后计划池（压力位卖点）

盘后（15:00 后）对最新交易日收盘定型数据跑 signals，选出「突破放量 / 缩量回踩」预选池，
给出**次日盘中买点（回踩支撑位低吸）**与**卖点（压力位止盈）**参考，次日盘中执行。

**买卖点机制（已回测验证）**
- **买点** = 次日盘中回踩支撑位附近低吸（`dip_buf`，±2% 缓冲，量比企稳才买，不回踩不买）。
  机械次日追开/收为负，改回踩低吸后翻正：突破放量 胜率 60% / +27.8%（持有10日，止损-8%）；
  缩量回踩 胜率 44% / +47.3%（持有20日，止损-5%）。
- **卖点** = 触及**压力位（前 60 日最高价；突破放量取 ×1.10）**附近即止盈。
  相对「固定持有」全面改善：胜率 32.6%→52%、收益 +16.15%→+22.89%、夏普 0.39→0.84、
  回撤 -14.27%→-6.72%（回测样本内）。

**前向验证（Forward Validation，确认样本外有效，非过拟合）**
- 脚本：`opt_study/volume_price_forward_validation.py`，严格零泄漏切分（样本内 ≤2026-01-31 选参，样本外 2026-02~08 测试）。
- 结论：压力位卖点相对固定持有**改善 +5.8pp、胜率 +18pp**（OOS 持有 -5.99% vs 压力位 -0.21%），
  样本内选出的最优配置 (sell_buf=0.02, cap=20) 在两段样本内均稳定最优 → 证实泛化。

**实盘链路（盘后计划 → 次日买入 → 压力位卖出提醒）**
1. 盘后 `volume-price-scan.yml`（cron-job.org 15:30 触发）产出计划池，登记为 `PLAN` 状态，
   `tp_price` = 压力位、`support` = 支撑位，Bark 推送「买点(支撑) / 卖点(压力)」。
2. 次日盘中：按量比确认**回踩支撑位附近 + 抛压衰竭**后低吸买入（人工判断，不自动下单）。
3. 买入后登记真实持仓（压力位卖点接入实盘提醒）：
   ```bash
   python add_manual_position.py <代码> <买入价> --vp
   ```
   `--vp` 自动从计划池读取该股「支撑/压力位」作为止损/止盈，登记为 `HOLDING` 真实持仓，
   并清除对应的 PLAN 计划记录（避免「计划提醒」与「实盘卖出提醒」重复推送）。
4. `position_monitor`（交易时段每 30 分钟）监控：价格**触及压力位**时推送
   `⚠️ 实际持仓·压力位卖出` 提醒，含买点(支撑)/卖点(压力)/当前价/盈亏。

**相关脚本**
- `tools/volume_price_scan.py` — 盘后扫描 + Bark（有信号推计划池 / 0 候选推回执）
- `opt_study/volume_price_entry_study.py` — 买点（回踩低吸）研究
- `opt_study/volume_price_exit_study.py` — 卖点（压力位）研究
- `opt_study/volume_price_forward_validation.py` — 前向验证（样本外）
- `docs/cron-job-volume-price-scan.md` — cron-job.org 配置模板

## 因子研究

**KDJ 底部金叉（不采纳）**
- 回溯 2024-07~2026-06 共 **6200 笔**低位绩优候选日，比对「KDJ 底部金叉(D<30)」与「无金叉」的 N 日收益
- 20 日（实际持有期）胜率：金叉组 62.9% vs 无金叉组 63.6%；均值收益 2.76% vs 2.94%
- Welch t 检验 p=0.74 → 无显著差异，且方向在各周期不一致
- 结论：低位绩优候选本身已深度超卖，KDJ 几乎必然在超卖区，金叉是近乎冗余的同步信号——砍候选池却无质量提升，故**不采纳为因子**

## 外部 Cron (cron-job.org)

尾盘: `.../momentum-scan.yml/dispatches` POST `{"ref":"main","inputs":{"run_mode":"full"}}` Cron `25 14 * * 1-5` Asia/Shanghai

竞价: `.../auction-scan.yml/dispatches` POST `{"ref":"main"}` Cron `25 1 * * 1-5` Asia/Shanghai

价量盘后计划池: `.../volume-price-scan.yml/dispatches` POST `{"ref":"main"}` Cron `30 15 * * 1-5` Asia/Shanghai

LightGBM 推理: **不设定时**（手动 `gh workflow run daily_inference.yml --ref main`，或 GitHub 网页 Run workflow）。
恢复定时的配置模板见 [`docs/cron-job-daily-inference.md`](docs/cron-job-daily-inference.md)。

> 详细模板（URL / Header / Body / 时区换算）见 [`docs/cron-job-volume-price-scan.md`](docs/cron-job-volume-price-scan.md)。
> 本仓库 GitHub 原生 `schedule` 触发器不可靠，上述作业均改由 cron-job.org 主触发，GitHub schedule 仅兜底。

> 竞价外部盘的相关 A 股清单（锂矿股 / 存储-HBM 股）集中在 `tools/auction_extra_config.json`，**无需改代码即可增删**；缺失时回退内置默认。相关 A 股高开正反馈复用竞价主扫描已抓的实时行情（零额外网络）。

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
| 实盘每天都有推荐, 回测却 1 笔/月 | 快照式筛选 vs 交易级回合模拟, 口径不同 | 正常; 见「统一监控与选股登记」说明 |
