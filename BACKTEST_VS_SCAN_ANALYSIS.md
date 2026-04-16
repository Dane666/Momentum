# 回测 (Backtest) vs 扫描 (Scan) 模块差异分析

## 执行日期: 2026-01-30
## 分析对象: 
- 回测模块: `tests/momentum/backtest/simulator.py` (MomentumBacktester)
- 扫描模块: `tests/momentum/core/scanner.py` (MarketScanner) + `tests/momentum/core/engine.py`

---

## 一、数据来源差异

### 1.1 行情数据来源

| 模块 | 数据源 | 说明 |
|------|--------|------|
| **Backtest** | 日K线历史数据 | 从`fetch_kline_from_api()`获取日K线，用于完整历史回放 |
| **Scan** | 实时行情 + K线混合 | `fetch_realtime_quotes()`获取实时行情，`batch_calculate_vol_ratio()`获取K线 |

### 1.2 价格数据差异

| 方面 | Backtest | Scan |
|------|----------|------|
| 买入价格 | 14:45真实价格(5分钟K线) | 当前实时价格(接近收盘) |
| MA计算 | 日K收盘价 | 日K收盘价 |
| 偏离度 | 日K收盘价与MA20比较 | 实时价格与MA20比较 |
| 时间准确性 | 精确到14:45分钟级 | 实时更新，精度秒级 |

### 1.3 成交额数据

| 模块 | 成交额来源 | 说明 |
|------|-----------|------|
| **Backtest** | 日K `amount` | 全天累计成交额 |
| **Scan** | 实时行情 | 当前累计成交额(14:45时) |

---

## 二、指标计算差异

### 2.1 量比 (Volume Ratio)

#### Backtest:
```
vr_t = daily_volume[T] / avg_volume[T-5:T-1]
数据源: 日K线的volume字段
时间点: 日K收盘时刻(15:00)
```

#### Scan:
```
初筛阶段:
  vol_ratio = batch_calculate_vol_ratio()
  数据源: K线历史数据(日K)
  时间点: 当天14:45 (当时累计成交量/过去5日平均)
  公式: current_volume / (avg_volume[-5:-1].mean())

显示阶段:
  vr_t = today['vol_ratio']  # 因子计算阶段结果
  数据源: 同上
```

**差异:**
- Backtest使用的是15:00全天累计成交量
- Scan初筛使用的是14:45的累计成交量
- Scan显示的量比是因子计算阶段的值，可能与初筛时不一致

### 2.2 RSI (相对强弱指数)

#### 两者计算方法一致:
```python
delta = closes.diff()
gain = delta.clip(lower=0)
loss = (-delta).clip(lower=0)
avg_gain = gain.rolling(14).mean()
avg_loss = loss.rolling(14).mean()
rs = avg_gain / avg_loss
rsi = 100 - (100 / (1 + rs))
```

**差异:**
- Backtest: 使用日K收盘价计算(15:00)
- Scan: 使用日K收盘价计算(15:00)
- ✅ 基本一致

### 2.3 Sharpe比率

#### 两者计算方法:
```
sharpe = (returns.mean() / returns.std()) * sqrt(252)
区间: 过去20天日K收益率
```

**差异:**
- Backtest: 使用14:45价格替换最后一日收盘价(如果有)
- Scan: 使用日K收盘价
- ⚠️ 略有差异，但整体方向一致

### 2.4 Momentum (动量)

#### Backtest:
```
mom_5 = (close[T] / close[T-5]) - 1
mom_20 = (close[T] / close[T-20] - 1) * (0.02 / volatility_20)
close[T] = 14:45价格 (如果有14:45数据)
```

#### Scan:
```
mom_5 = (close[-1] / close[-5]) - 1
mom_20 = (close[-1] / close[-20] - 1) * (0.02 / volatility_20)
close[-1] = 日K收盘价
```

**差异:**
- Backtest: 用14:45价格替换日K收盘价
- Scan: 只用日K收盘价
- ⚠️ 这导致Backtest的momentum比Scan更准确

### 2.5 偏离度 (Bias)

#### 两者:
```
bias = (current_price / ma20) - 1
```

**差异:**
- Backtest: `current_price = 14:45真实价格`
- Scan: `current_price = 实时价格(14:45前后)`
- ✅ 基本相同(都在14:45左右)

---

## 三、筛选条件差异

### 3.1 初始候选筛选

| 筛选项 | Backtest | Scan | 备注 |
|--------|----------|------|------|
| 涨幅范围 | N/A(历史回放) | MIN_CHANGE_PCT~MAX_CHANGE_PCT | 4%~9.2% |
| 量比 | 无显式筛选 | MIN_VOL_RATIO ≥ 1.2 | ⚠️ |
| 成交额 | MIN_AMOUNT | MIN_AMOUNT | 2亿元 |
| 市值 | 无限制 | 无限制 | |
| ST股 | 无限制 | 排除 | Scan在获取实时行情时排除 |

**关键差异:**
- **Backtest没有量比筛选!** 只在后期阶段进行RSI/Sharpe筛选
- Scan在初筛阶段就进行量比筛选，筛掉低量比股票

### 3.2 因子筛选阈值

| 指标 | Backtest | Scan |
|------|----------|------|
| Sharpe | MIN_SHARPE > 1.0 | MIN_SHARPE > 1.0 |
| RSI | RSI > RSI_DANGER_ZONE(85) 删除 | RSI < RSI_DANGER_ZONE(80) 过滤 |
| Alpha | 无硬阈值 | 排名前N个 |
| 行业分散 | MAX_SECTOR_PICKS=1 | MAX_SECTOR_PICKS=1 |
| 总选股数 | MAX_TOTAL_PICKS=1 | MAX_TOTAL_PICKS=1 |

**差异:**
- RSI阈值不同: Backtest是85, Scan是80
- Scan在初筛时就排除RSI过高的，Backtest在后期排除

### 3.3 市场防御机制

| 机制 | Backtest | Scan |
|------|----------|------|
| 市场宽度检查 | 无 | MARKET_BREADTH_DEFENSE = 0.22 |
| 外资情绪检查 | 无 | 检查北向资金流向 |
| 市场情绪防御 | 无 | 有(涨停板数、连板数等) |

**差异:**
- Scan有市场防御机制，Backtest没有
- Backtest是历史回放，不需要实时防御
- Scan在市场不好时会躲避(返回空)

---

## 四、NLP与行业分散差异

### 4.1 NLP分析

| 模块 | NLP处理 |
|------|---------|
| **Backtest** | 无(历史回放不需要) |
| **Scan** | 有(基于当日新闻情绪) |

### 4.2 行业中性化

#### Backtest:
```python
def _industry_neutralization(df):
    # 去除sector NaN
    # 按sector分组，计算group_alpha(= score - group_mean_score)
    # 在group内排序，选出top股票
```

#### Scan (engine.py):
```python
def industry_neutralization_with_trend(df):
    # 同上逻辑
    # 另外加入trend因子(价格趋势)
    # 更复杂的中性化实现
```

**差异:**
- Scan的行业中性化更复杂，加入了trend因子
- Backtest是基础的sector分组排序

---

## 五、选股流程对比

### Backtest流程:
```
1. 加载历史K线数据(全市场)
2. 对每个交易日:
   a. 遍历全市场股票
   b. 计算日因子(mom, sharpe, rsi等) - 使用14:45价格
   c. 成交额筛选(MIN_AMOUNT)
   d. 行业中性化(sector分组)
   e. RSI/Sharpe筛选
   f. 行业分散选股(max_sector_picks=1)
   g. 模拟前向收益
3. 汇总统计

特点: 完整回放, 不考虑市场环境
```

### Scan流程:
```
1. 获取实时行情(全市场) - fetch_realtime_quotes()
2. 市场宽度检查 - 如果<22%返回空
3. 市场情绪检查 - 外资、连板等
4. 涨幅筛选(4~9.2%)
5. 量比筛选(>=1.2) - batch_calculate_vol_ratio()
6. 成交额筛选(MIN_AMOUNT)
7. 量化因子计算(60个候选)
8. RSI筛选(<80) - 在因子计算后
9. Sharpe筛选(>1.0)
10. NLP分析(top 10)
11. 行业分散选股
12. 显示报告

特点: 实时防御, 考虑市场环境
```

---

## 六、关键问题总结

### 6.1 ⚠️ Backtest没有量比筛选

**问题:**
- Backtest在历史回放时没有对量比进行筛选
- Scan初筛时过滤量比<1.2的股票
- 导致Backtest可能选出低量比的股票，而Scan会过滤掉

**影响:**
- Backtest的股票池比Scan更大
- 可能导致两者结果不一致

**建议:**
- Backtest也应该加入量比筛选(>=1.2)
- 或Scan降低量比阈值到与Backtest一致

### 6.2 ⚠️ 价格数据时间点差异

**问题:**
```
Backtest: 14:45价格 (5分钟K线)
Scan显示: 实时价格(14:45后的实时更新)
```

**影响:**
- Scan显示的量比(1.1, 1.5)与初筛计算(2.8, 2.9)不一致
- 是因为显示用的是因子计算阶段的vr_t，而不是初筛的量比

**建议:**
- ✅ 已修复: 用初筛量比替换因子阶段的vr_t值

### 6.3 ⚠️ RSI阈值差异

**问题:**
```
Backtest: RSI > 85 删除(允许0~85)
Scan: RSI < 80 过滤(只允许0~80)
```

**影响:**
- Scan更严格(80 vs 85)
- Scan在初期就过滤，Backtest在后期过滤

**建议:**
- 统一RSI阈值到80
- 都在初期过滤(优化效率)

### 6.4 ⚠️ Momentum计算差异

**问题:**
```
Backtest: mom = (price_1445 / prev_price) - 1
Scan: mom = (close / prev_close) - 1
```

**影响:**
- Backtest用14:45实时价格，更准确
- Scan用日K收盘价，延迟1秒

**建议:**
- Scan也应该用14:45附近的价格计算momentum
- 或Backtest改用日K收盘价保持一致

### 6.5 ⚠️ 市场防御机制差异

**问题:**
```
Backtest: 无市场防御(完整回放历史)
Scan: 有市场防御(22%宽度阈值)
```

**影响:**
- Backtest在2015年股灾期间也会选股
- Scan在市场极度恶劣时会躲避

**建议:**
- Backtest可选择是否加入市场防御机制(可配置)
- 或两者都加上市场防御(更接近实盘)

---

## 七、数据来源一致性

### 问题追踪: 新浪接口的假量比

**发现:**
- 新浪接口的`量比`字段硬编码为1.0(不提供真实数据)
- 导致初筛时量比都是1.0，无法过滤

**解决方案:**
- ✅ 已实现: batch_calculate_vol_ratio()从K线实时计算
- ✅ 已实现: 初筛阶段使用真实量比替换假量比
- ✅ 已实现: 显示阶段用初筛量比替换因子量比

**文件修改:**
- `data/fetcher.py`: batch_calculate_vol_ratio()
- `core/scanner.py`: _filter_candidates()
- `core/engine.py`: _display_report()

---

## 八、建议改进清单

| 优先级 | 类型 | 描述 | 状态 |
|--------|------|------|------|
| 🔴高 | Backtest | 加入量比筛选(>=1.2) | ❌ 需要修改 |
| 🔴高 | RSI | 统一阈值到80 | ⚠️ 部分修改 |
| 🟡中 | Momentum | Scan也使用14:45价格 | ❌ 需要修改 |
| 🟡中 | Backtest | 可选的市场防御机制 | ❌ 需要修改 |
| 🟢低 | 文档 | 补充架构设计文档 | ✅ 本文档 |

---

## 九、测试验证方案

```
1. 对同一批历史日期(如2026-01-20 ~ 2026-01-30)
   - Backtest选出的股票列表
   - Scan当天选出的股票列表
   - 比较是否一致

2. 验证指标值:
   - 量比: 2.8, 2.9 (Backtest应显示同值)
   - RSI: 71, 76 (应该一致)
   - Sharpe: 5.07, 5.72 (应该一致)

3. 验证筛选流程:
   - 全市场3207 -> 涨幅130 -> 量比105 -> ...
   - Backtest应该有相同的漏斗统计
```

---

## 附录: 配置对比

```python
# config.py 中的关键参数

# 共同参数:
MIN_CHANGE_PCT = 4.0
MAX_CHANGE_PCT = 9.2
MIN_VOL_RATIO = 1.2
MIN_AMOUNT = 200000000
MIN_SHARPE = 1.0
MAX_SECTOR_PICKS = 1
MAX_TOTAL_PICKS = 1

# Backtest特有:
BACKTEST_DAYS_DEFAULT = 120
HOLD_PERIOD_DEFAULT = 5
SLIPPAGE = 0.001
ATR_STOP_FACTOR = 1.2

# Scan特有:
MARKET_BREADTH_DEFENSE = 0.22
RSI_DANGER_ZONE = 80.0
NLP_CANDIDATE_SIZE = 10
ENABLE_NLP_ANALYSIS = True
```

---

**报告生成时间:** 2026-01-30 15:10
**分析对象:** Momentum v16 量化选股系统
**分析深度:** 模块级、指标级、数据级
