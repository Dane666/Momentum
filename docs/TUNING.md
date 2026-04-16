# 参数调优指南

本文档介绍如何对Momentum v16策略进行参数优化。

---

## 目录

1. [核心参数说明](#核心参数说明)
2. [参数优化方法](#参数优化方法)
3. [回测验证流程](#回测验证流程)
4. [常见问题与调优](#常见问题与调优)
5. [最佳实践](#最佳实践)

---

## 核心参数说明

### 1. 持仓参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `hold_period` | 5 | 3-10 | 最大持仓天数 |
| `max_positions` | 5 | 3-10 | 最大持仓数量 |
| `position_size` | 0.20 | 0.10-0.33 | 单只股票仓位比例 |

**调优建议**:
- 短线激进: `hold_period=3`, `max_positions=8`
- 稳健配置: `hold_period=5`, `max_positions=5`
- 中期持有: `hold_period=8`, `max_positions=4`

---

### 2. 止盈止损参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `stop_loss` | 0.05 | 0.03-0.10 | 止损百分比 |
| `take_profit` | 0.10 | 0.06-0.20 | 止盈百分比 |
| `trailing_stop` | 0.05 | 0.03-0.08 | 移动止损 |

**调优矩阵**:

| 市场环境 | 止损 | 止盈 | 备注 |
|----------|------|------|------|
| 低波动 | 3% | 6% | 收紧止损止盈 |
| 正常 | 5% | 10% | 默认配置 |
| 高波动 | 8% | 8% | 放宽止损，收紧止盈 |

---

### 3. 选股参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `rsi_upper` | 80 | 70-85 | RSI上限 |
| `rsi_lower` | 30 | 20-35 | RSI下限 |
| `vol_ratio_min` | 1.0 | 0.8-1.5 | 最小量比 |
| `bias_upper` | 0.20 | 0.15-0.25 | 乖离率上限 |

---

### 4. Alpha权重参数

```python
# 默认权重
WEIGHTS = {
    'momentum': 0.25,
    'technical': 0.20,
    'volume': 0.20,
    'flow': 0.20,
    'sentiment': 0.15,
}
```

**调优方法**: 使用网格搜索或贝叶斯优化。

---

## 参数优化方法

### 方法1: 网格搜索

```python
from tests.momentum.backtest.period_optimizer import HoldPeriodOptimizer

optimizer = HoldPeriodOptimizer(db_path='qlib_pro_v16.db')

# 测试不同持仓周期
results = optimizer.run_multi_period_test(periods=[3, 4, 5, 6, 7, 8])

# 获取推荐配置
best_period, reason = optimizer.recommend_period(results)
print(f"推荐持仓周期: {best_period}天 ({reason})")
```

### 方法2: 参数扫描

```python
from itertools import product

# 参数空间
param_grid = {
    'hold_period': [3, 4, 5, 6],
    'stop_loss': [0.03, 0.05, 0.07],
    'take_profit': [0.08, 0.10, 0.12],
}

results = []
for hp, sl, tp in product(
    param_grid['hold_period'],
    param_grid['stop_loss'],
    param_grid['take_profit']
):
    # 运行回测
    metrics = run_backtest(hold_period=hp, stop_loss=sl, take_profit=tp)
    results.append({
        'hold_period': hp,
        'stop_loss': sl,
        'take_profit': tp,
        'sharpe': metrics.sharpe,
        'total_return': metrics.total_return,
    })

# 按夏普比率排序
results_df = pd.DataFrame(results).sort_values('sharpe', ascending=False)
print(results_df.head(10))
```

### 方法3: 贝叶斯优化

```python
from bayes_opt import BayesianOptimization

def objective(hold_period, stop_loss, take_profit):
    metrics = run_backtest(
        hold_period=int(hold_period),
        stop_loss=stop_loss,
        take_profit=take_profit
    )
    return metrics.sharpe

optimizer = BayesianOptimization(
    f=objective,
    pbounds={
        'hold_period': (3, 8),
        'stop_loss': (0.03, 0.10),
        'take_profit': (0.06, 0.15),
    }
)

optimizer.maximize(init_points=5, n_iter=20)
print(f"最优参数: {optimizer.max}")
```

---

## 回测验证流程

### Step 1: 样本内优化

```
时间段: 2023-01-01 ~ 2024-06-30 (18个月)
目标: 找到最优参数组合
```

### Step 2: 样本外验证

```
时间段: 2024-07-01 ~ 2024-12-31 (6个月)
目标: 验证参数稳定性
```

### Step 3: 滚动验证

```python
# 滚动窗口回测
windows = [
    ('2023-01', '2023-06', '2023-07', '2023-09'),  # Train → Test
    ('2023-04', '2023-09', '2023-10', '2023-12'),
    ('2023-07', '2023-12', '2024-01', '2024-03'),
    # ...
]

for train_start, train_end, test_start, test_end in windows:
    # 样本内优化
    best_params = optimize(train_start, train_end)
    
    # 样本外测试
    test_metrics = backtest(test_start, test_end, **best_params)
    
    print(f"Window {test_start}-{test_end}: Sharpe={test_metrics.sharpe:.2f}")
```

---

## 常见问题与调优

### 问题1: 胜率过低

**症状**: 胜率 < 45%

**可能原因**:
- 止损过紧
- 选股条件过松
- 市场环境不适合

**调优方案**:
```python
# 放宽止损
stop_loss = 0.07

# 收紧选股
rsi_upper = 75
vol_ratio_min = 1.2
```

---

### 问题2: 回撤过大

**症状**: 最大回撤 > 30%

**可能原因**:
- 止盈过晚
- 持仓过于集中
- 未及时止损

**调优方案**:
```python
# 收紧止盈
take_profit = 0.08

# 增加持仓数量分散风险
max_positions = 8

# 添加移动止损
trailing_stop = 0.05
```

---

### 问题3: 盈亏比过低

**症状**: 平均盈利 / 平均亏损 < 1.5

**调优方案**:
```python
# 放宽止盈
take_profit = 0.12

# 收紧止损
stop_loss = 0.04
```

---

### 问题4: 交易过于频繁

**症状**: 日均交易 > 10 笔

**调优方案**:
```python
# 延长持仓周期
hold_period = 7

# 提高选股门槛
alpha_threshold = 0.6
```

---

## 最佳实践

### 1. 参数稳定性检验

好的参数应该在邻近区域也表现良好：

```python
def check_param_stability(base_params, metric='sharpe'):
    """检查参数稳定性"""
    base_result = backtest(**base_params)
    
    variations = []
    for param, value in base_params.items():
        for delta in [-0.1, 0.1]:  # ±10%变化
            new_params = base_params.copy()
            new_params[param] = value * (1 + delta)
            result = backtest(**new_params)
            variations.append(result[metric])
    
    stability = 1 - np.std(variations) / np.mean(variations)
    return stability  # 越接近1越稳定
```

### 2. 避免过拟合

1. **使用样本外数据验证**
2. **交叉验证**
3. **限制参数数量** - 不要同时优化太多参数
4. **设置合理的参数边界**

### 3. 版本管理

使用 `ConfigVersionManager` 管理参数版本：

```python
from tests.momentum.config_manager import ConfigVersionManager

manager = ConfigVersionManager()

# 保存当前版本
manager.create_version(
    name='v16.1_aggressive',
    hold_period=3,
    stop_loss=0.07,
    take_profit=0.08,
    description='激进短线配置'
)

# 对比版本
diff = manager.compare_versions('v16.0', 'v16.1_aggressive')
print(diff)

# 回滚
manager.rollback('v16.0')
```

### 4. 定期复盘

建议每月复盘一次：

1. 检查各参数表现
2. 对比基准收益
3. 分析失败案例
4. 调整参数或策略

---

## 附录: 推荐参数配置

### 配置A: 稳健型

```python
CONFIG_STABLE = {
    'hold_period': 5,
    'max_positions': 5,
    'stop_loss': 0.05,
    'take_profit': 0.10,
    'trailing_stop': 0.05,
    'rsi_upper': 75,
    'vol_ratio_min': 1.0,
}
```

### 配置B: 激进型

```python
CONFIG_AGGRESSIVE = {
    'hold_period': 3,
    'max_positions': 8,
    'stop_loss': 0.07,
    'take_profit': 0.08,
    'trailing_stop': 0.04,
    'rsi_upper': 80,
    'vol_ratio_min': 1.2,
}
```

### 配置C: 保守型

```python
CONFIG_CONSERVATIVE = {
    'hold_period': 7,
    'max_positions': 4,
    'stop_loss': 0.04,
    'take_profit': 0.12,
    'trailing_stop': 0.06,
    'rsi_upper': 70,
    'vol_ratio_min': 0.8,
}
```
