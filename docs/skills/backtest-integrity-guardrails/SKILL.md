---
name: backtest-integrity-guardrails
agent_created: true
description: 量化回测/策略研究中防止两类经典翻车的两套护栏——(1)回测窗口严格截断：本地 K 线库常在某个日期后残缺(如本仓 2026-07-01 起仅数十只 vs 正常 ~2950 只), 若不清算会把净值伪造成 +20% 以上的荒谬值; (2)配对 t 检验(scipy.stats.ttest_rel)：同一批信号只换一条规则(买点/卖点/阈值)时, 用配对检验区分"真改进"与"噪声", 避免被均值陷阱误导。适用于任何 A 股/量化回测、策略对比、卖点买点规则研究。
---

# 回测完整性护栏（Backtest Integrity Guardrails）

## 何时用

- 回测 / 策略对比 / 卖点·买点规则研究（如 `tasks/model_inference/07_exit_rule_study.py`）。
- 本地 K 线库可能不是"全量"——跑过 CI 增量补数、或本地只抓了部分票，导致**某日期之后股票数断崖式下跌**。
- 你想证明"A 规则比 B 规则好"时——尤其是同一批信号只改一个变量。

## 护栏一：回测窗口严格截断（防残缺库污染）

### 触发信号
先看本地库每交易日覆盖的股票数。如果出现：

| 日期 | 覆盖股票数 | 是否正常 |
|---|---|---|
| 2026-06-30 | ~2950 | ✅ |
| 2026-07-01 | 2337 | ⚠️ 开始残缺 |
| 2026-07-15 | 2042 | ❌ |
| 2026-07-29 | 28 | ❌❌ 几乎空库 |

→ **回测 `TEST_END` 必须设在最后一个"完整"交易日之前**（本仓取 `2026-06-30`）。任何交易、净值、配对检验都**不得触碰残缺段**。

### 代码模式（来自 07_exit_rule_study.py）

```python
TEST_START = '2026-01-01'
TEST_END   = '2026-06-30'   # 必须是"完整库"的最后一日

# 1) 单笔模拟: 持仓不得越过 TEST_END 对应行号(其后数据残缺)
def simulate_one(g, i, rule_kw, stop_i=None):
    ...
    if stop_i is not None:
        end = min(end, stop_i)          # 截断
    ...
    truncated = stop_i is not None and end == stop_i and end < j + cap

# 2) 预计算每个 tick 在 TEST_END 的行号
ts_end = pd.Timestamp(TEST_END)
stop_idx = {}
for c, g in ctx.items():
    p = g.index.searchsorted(ts_end, side='right') - 1
    if p >= 0:
        stop_idx[c] = int(p)

# 3) 滚动资金池: 窗口末日强制平仓(reason='window_end'), 不延续到残缺段
def simulate_pool(picks_by_day, ctx, dates, rule_kw, max_pos=5):
    last_k = len(dates) - 1
    for k, d in enumerate(dates):
        ...
        if k == last_k:                  # 窗口末日
            for p in list(pos):
                ... px = g['close'].iat[kk]
                cash += p['shares'] * px * (1 - COST/2)
                trades.append(dict(... reason='window_end'))
                pos.remove(p)
            nav.append(cash); break
```

> ⚠️ **不要**用 `SETTLE_TAIL` 之类"再多跑 N 天清算"的参数把净值算进残缺段——那正是把净值伪造成 +21% 的根因。残缺段一律不碰。

## 护栏二：配对 t 检验（区分真改进 vs 噪声）

### 为什么需要
同一批信号（例如 555 笔）只换卖点规则，两规则的单笔收益**天然可配对**。直接比"均笔 +0.65% vs +1.43%"会被**均值陷阱**骗：
- `volsig_sl8` 均笔 +1.43%，但**中位 −8.35%**（少数大赢家撑起均值）。
- 必须同时看 **中位 / 去尾均值（trimmed mean）**，不能只看均值。

配对检验回答的是："Δ均笔 是否显著 ≠ 0"——显著才叫真改进，否则是噪声。

### 代码模式

```python
from scipy import stats as sps
import numpy as np

# raw[name] = { (code, signal_i): 单笔收益 }  同一批信号, 不同规则
base = raw['hold_10']
for name, rmap in raw.items():
    if name == 'hold_10':
        continue
    keys = sorted(set(base) & set(rmap))     # 配对键(同信号)
    if len(keys) < 30:                        # 样本太少不检验
        continue
    d = np.array([rmap[k] - base[k] for k in keys])
    t, p = sps.ttest_rel([rmap[k] for k in keys],
                         [base[k] for k in keys])
    # p < 0.05 → 显著更差(若 Δ<0)或显著更好(若 Δ>0)
    # p 不显著 → 噪声, 别当改进
```

### 判读阈值
| p 值 | 结论 |
|---|---|
| < 0.01 | `***` 高度显著 |
| < 0.05 | `**` 显著（真改进 / 真更差） |
| < 0.10 | `*` 边际 |
| ≥ 0.10 | 噪声，当没发生 |

> 本仓实证：所有"压力位/均线/价量/移动止盈 比 hold_10 更好"的假设 **p 均 ≥ 0.13（不显著）**；而 `trail7`（移动止盈）Δ−2.82% **p<0.001 显著更差**。→ 固定持有最优，形态卖点是噪声/负贡献。

## 护栏三（加固）：稳健性扫描，拒绝参数依赖噪声

一个规则若只在 `max_pos=5` 时好看、换 `max_pos=8` 直接崩，就是**参数依赖噪声**，不可信。
- 稳健性 A：资金容量 `max_pos ∈ {3, 5, 8}` 下结论是否一致。
- 稳健性 B：前后半窗拆分，结论是否都稳。
- 本仓 `ma20_break_sl8` 在 max_pos=5 时 +48.6%，max_pos=8 时 +0.01% → 纯噪声，丢弃。`hold_10` 在三者下 +11.5% / +11.2% / +15.2% 稳定为正 → 可信。

## 输出清单（研究脚本应产出的）
1. 窗口声明（明确 `TEST_END` 及"为何截断"）。
2. Layer1 按笔：胜率 / 均笔 / **中位** / **去尾均值** / 夏普。
3. Layer3 配对 t 检验表（Δ均笔 + p 值）。
4. Layer2 滚动资金池（可变持有期、最多同时持 N 只、卖出释放即补——这才是"实盘滚动"）。
5. 稳健性 A/B。

## 反模式（别再做）
- ❌ 回测窗口偷偷延伸到残缺库段（净值虚高）。
- ❌ 只看均笔/均值下结论（被极端值骗）。
- ❌ 用"不同信号集"比两条规则（不可配对，结论无效）。
- ❌ 把只在某一参数下好看的结果当泛化结论。

## 参考实现
- `tasks/model_inference/07_exit_rule_study.py` — 上述护栏的完整参考（RULES 字典、simulate_one/simulate_pool、配对检验、稳健性扫描）。
- 产物：`tasks/model_inference/output/exit_rule_study.json`。
