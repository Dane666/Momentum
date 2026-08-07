# Momentum v16 策略系统

## 文档总览

| 文档 | 内容 | 何时看 |
|------|------|--------|
| [README.md](README.md) | 本文档 — 模块结构、快速开始、参数配置 | 了解系统全貌 |
| [FACTORS.md](FACTORS.md) | 因子定义与计算口径 | 新增/调试因子 |
| [TUNING.md](TUNING.md) | 参数调优指南 | 优化策略参数 |
| [GRID_TRADING_GUIDE.md](GRID_TRADING_GUIDE.md) | 网格交易策略说明 | 使用网格策略 |
| [GRID_TRADING_OPERATION_GUIDE.md](GRID_TRADING_OPERATION_GUIDE.md) | 网格交易实操手册 | 网格实盘操作 |
| [cron-job-volume-price-scan.md](cron-job-volume-price-scan.md) | 价量扫描的 cron-job.org 主触发配置 | 配置每日 15:30 价量扫描 |
| [cron-job-daily-inference.md](cron-job-daily-inference.md) | LightGBM 推理的 cron-job.org 配置模板（**当前不启用**，手动触发） | 日后想恢复定时推理 |

### 定时触发说明

> **重要**：GitHub 原生 `schedule` 在本仓库已确诊不可靠（2026-08-04，cron 事件常不派发）。
> 需要稳定定时的工作流一律用 **cron-job.org → POST `workflow_dispatch` API** 作为主触发，
> GitHub `schedule` 仅作兜底。配置模板见上表两个 `cron-job-*.md`。

| 工作流 | 触发方式 | 说明 |
|--------|----------|------|
| `volume-price-scan.yml` | cron-job.org 15:30 CST（主）+ GitHub schedule（兜底） | 价量口诀策略盘后扫描 |
| `daily_inference.yml` | **仅手动** `workflow_dispatch` | LightGBM 模型推荐，观察期内不定时推送 |
| `momentum-scan.yml` | cron-job.org（主） | 尾盘动量选股 |
| `position-monitor.yml` | schedule 每 30min | 持仓止盈止损监控 |

## 概述

Momentum v16 是一个基于动量和Alpha因子的短线量化交易策略系统，
采用多因子选股 + 规则化退出的框架。

## 项目结构

```
tests/momentum/
├── config.py              # 全局配置
├── config_manager.py      # 参数版本管理
├── main.py                # 主入口
│
├── alpha/                 # Alpha因子模块
│   ├── alpha_model.py     # Alpha合成模型
│   └── weight_config.py   # 权重配置系统
│
├── backtest/              # 回测模块
│   ├── simulator.py       # 回测模拟器
│   ├── engine.py          # 策略引擎
│   ├── metrics.py         # 指标计算
│   └── period_optimizer.py # 持仓周期优化
│
├── data/                  # 数据模块
│   ├── cache.py           # K线缓存
│   └── hierarchical_cache.py # 分层缓存
│
├── factors/               # 因子模块
│   └── technical_factors.py # 技术因子
│
├── risk/                  # 风控模块
│   ├── exit_rules.py      # 退出规则
│   └── adaptive_exit.py   # 自适应止损
│
├── monitor/               # 监控模块
│   └── health_check.py    # 健康检查
│
├── tests/                 # 单元测试
│   ├── conftest.py        # pytest配置
│   ├── test_factors.py    # 因子测试
│   ├── test_metrics.py    # 指标测试
│   └── test_backtest.py   # 回测测试
│
└── docs/                  # 文档
    ├── README.md          # 本文档
    ├── FACTORS.md         # 因子说明
    └── TUNING.md          # 调优指南
```

## 快速开始

### 1. 运行回测

```python
from tests.momentum.backtest.simulator import MomentumBacktester
from tests.momentum import config

# 初始化回测器
backtester = MomentumBacktester(
    initial_capital=config.INITIAL_CAPITAL,
    max_positions=config.MAX_POSITIONS,
)

# 运行回测
results = backtester.run(
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# 查看结果
print(f"总收益: {results['total_return']:.2%}")
print(f"夏普比率: {results['sharpe']:.2f}")
print(f"最大回撤: {results['max_drawdown']:.2%}")
```

### 2. 实盘选股

```python
from tests.momentum.core.scanner import MomentumScanner

scanner = MomentumScanner()

# 获取今日推荐
signals = scanner.scan_today()

for signal in signals:
    print(f"{signal['code']} {signal['name']} - Alpha: {signal['alpha']:.2f}")
```

### 3. 健康监控

```python
from tests.momentum.monitor.health_check import quick_health_check

status = quick_health_check(db_path='qlib_pro_v16.db')

print(f"系统状态: {status.overall}")
print(f"当前胜率: {status.win_rate:.1%}")
print(f"当前回撤: {status.drawdown:.1%}")

if status.alerts:
    print("\n告警信息:")
    for alert in status.alerts:
        print(f"  [{alert.level.value}] {alert.message}")
```

## 核心模块说明

### Alpha因子模型

策略使用多因子合成的Alpha分数进行选股：

```python
from tests.momentum.alpha.weight_config import get_weight_config

# 使用预设权重
weights = get_weight_config('momentum_first')

# 或自定义权重
from tests.momentum.alpha.weight_config import AlphaWeightConfig
custom_weights = AlphaWeightConfig(
    momentum=0.35,
    technical=0.25,
    volume=0.15,
    flow=0.15,
    sentiment=0.10,
)
```

### 自适应止损

根据市场状态动态调整止盈止损：

```python
from tests.momentum.risk.adaptive_exit import AdaptiveExitEngine

engine = AdaptiveExitEngine()

params = engine.get_adaptive_params(
    atr_pct=2.5,       # ATR占价格百分比
    rsi=72,            # 当前RSI
    bias=0.12,         # 乖离率
    market_condition='bullish'
)

print(f"建议止损: {params.stop_loss_pct:.1%}")
print(f"建议止盈: {params.take_profit_pct:.1%}")
print(f"原因: {params.reason}")
```

### 分层缓存

三级缓存提升数据访问效率：

```python
from tests.momentum.data.hierarchical_cache import HierarchicalCache

cache = HierarchicalCache(
    db_path='qlib_pro_v16.db',
    l1_maxsize=200,        # 内存缓存200只股票
    l2_ttl_hours=24,       # 磁盘缓存24小时
)

# 获取数据（自动穿透各层缓存）
df = cache.get_kline('000001', start_date='2024-01-01')

# 查看缓存统计
print(cache.stats())
```

## 参数配置

主要参数在 `config.py` 中配置：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `INITIAL_CAPITAL` | 1,000,000 | 初始资金 |
| `MAX_POSITIONS` | 5 | 最大持仓数 |
| `HOLD_PERIOD` | 5 | 持仓天数 |
| `STOP_LOSS` | 0.05 | 止损比例 |
| `TAKE_PROFIT` | 0.10 | 止盈比例 |
| `SLIPPAGE` | 0.008 | 滑点 |

详细调优指南请参考 [TUNING.md](TUNING.md)。

## 运行测试

```bash
# 运行所有测试
pytest tests/momentum/tests/ -v

# 运行特定测试
pytest tests/momentum/tests/test_factors.py -v

# 生成覆盖率报告
pytest tests/momentum/tests/ --cov=tests/momentum --cov-report=html
```

## 数据库结构

系统使用SQLite存储数据，主要表：

| 表名 | 说明 |
|------|------|
| `kline_cache` | K线数据缓存 |
| `backtest_sessions` | 回测会话记录 |
| `backtest_trades` | 交易记录 |
| `factor_logs` | 因子日志 |

## 更新日志

### v16.1 (2025-01)
- 新增自适应止损模块
- 新增分层缓存架构
- 新增健康监控系统
- 完善单元测试覆盖
- 文档完善

### v16.0 (2024-12)
- 初始版本
- 基于动量因子的短线策略
- 多因子Alpha选股
