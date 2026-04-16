# 网格交易（均值回归）策略示例

> 风险提示：网格策略在单边趋势市中可能持续亏损，请务必设置止损，并先在模拟盘中验证。

## 1. 功能概览

该示例实现了一个适合初学者阅读的网格交易回测框架，包含：

- 数据获取：使用 `yfinance` 下载日线数据（开盘、收盘、成交量）
- 策略逻辑：
  - 以起始日 `N` 日均价（可配置）作为网格中枢
  - 按固定网格间距构建上下对称网格
  - 价格下穿网格买入、上穿网格卖出
  - 设置仓位上限（默认 80% 资金对应仓位）
  - 设置破网止损（跌破最下网格线后再下跌 `X%` 强制清仓）
- 回测与绩效：
  - 累计收益率、年化收益率、最大回撤、夏普比率、胜率
- 可视化：
  - 价格+网格线+买卖点
  - 策略净值 vs 标的净值
  - 回撤曲线

---

## 2. 安装依赖

在项目根目录执行：

```bash
pip install yfinance pandas matplotlib numpy
```

如果你使用虚拟环境，请先激活环境再安装。

---

## 3. 运行方式

脚本路径：

- `tests/momentum/grid_mean_reversion_demo/grid_trading_strategy.py`

在项目根目录运行：

```bash
python tests/momentum/grid_mean_reversion_demo/grid_trading_strategy.py
```

运行后会输出：

1. 终端中的绩效表格
2. 最近交易记录（若有）
3. 三张回测图（价格+网格、净值对比、回撤）

---

## 4. 如何修改参数（核心）

直接修改脚本底部的 `config` 字典，无需改核心逻辑。

```python
config = {
    "symbol": "510300.SS",       # 标的，如 "SPY"
    "start_date": "2020-01-01",
    "end_date": "2023-12-31",
    "init_capital": 100000,
    "grid_num": 10,               # 上下各5层
    "grid_spacing": 0.02,         # 网格间距2%
    "trade_unit": 100,            # 每次每层100股
    "stop_loss_pct": 0.10,        # 破网后再跌10%止损
    "ma_window": 20,              # 20日均线作为中枢
    "commission_rate": 0.00025,
    "stamp_duty_rate": 0.001,
    "max_position_ratio": 0.8,    # 最大仓位80%
    "use_ma_center": True,
}
```

常见调参方向：

- 增大 `grid_spacing`：交易频率下降，单笔波动空间更大
- 减小 `grid_spacing`：交易频率上升，手续费影响更明显
- 增大 `grid_num`：可覆盖更宽区间，但资金占用更高
- 调整 `trade_unit`：直接影响每次下单规模和回撤速度

---

## 5. 如何解读回测结果

重点先看两个指标：

1. **最大回撤**：衡量最糟糕时期净值回落幅度，越小越稳健。
2. **胜率**：已平仓卖出交易中盈利占比；胜率高不代表一定赚钱，还要结合盈亏比。

再结合：

- **累计收益率 / 年化收益率**：观察收益水平
- **夏普比率**：考虑波动后的风险调整收益
- **净值曲线与回撤曲线**：是否平稳、是否有长期低迷区间

---

## 6. 代码结构说明

- `DataFetcher`：下载与清洗行情数据、计算均线
- `GridTradingStrategy`：
  - `calculate_grid_levels()` 计算网格线
  - `generate_signals()` 生成买卖信号
  - `execute_trades()` 执行撮合、更新现金与持仓
  - `evaluate_performance()` 计算绩效指标
  - `plot_results()` 绘图
  - `backtest()` 一键串联完整流程

此外预留了扩展接口：

- `adjust_grid_dynamically()`：可扩展动态网格
- `rotate_multi_assets()`：可扩展多标的轮动

---

## 7. 注意事项

- 网格策略不适合所有市场阶段，尤其怕单边趋势。
- 若回测几乎无交易，通常是网格间距过大或回测区间波动不足。
- 不同市场（A股/美股）手续费、税费规则不同，实盘前请按券商规则修正参数。
