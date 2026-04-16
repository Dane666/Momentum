# 数据导入模块（仅保留导入功能）

当前仅保留实盘数据导入功能，后续在数据导入完善后再进行对账功能开发。

## 功能

- CSV 批量导入交易记录
- ✨ **自动计算持仓**：从交易记录自动计算当前持仓
- ✨ **自动计算收益**：从持仓自动计算每日收益
- 交互式手工录入交易
- 生成示例 CSV 模板
- 手动触发计算（可选）：`--calc-positions` 和 `--calc-pnl`

## 设计理念

**推荐方式**：只需导入交易记录，持仓和收益自动计算

- **交易记录** 是源头数据，记录每笔买卖操作
- **持仓** 从交易记录自动计算：按股票代码汇总买入成本，减去卖出数量
- **每日收益** 从持仓自动计算：每日持仓市值变化

**自动化流程**：

```
导入交易记录 → 自动计算持仓 → 自动计算收益
```

每次导入交易记录或交互式录入后，系统会自动更新持仓和收益，无需手动操作。

## 快速开始

### 1. 生成示例模板

```bash
python3 import_live_data.py --sample
```

### 2. 导入交易记录（推荐）

```bash
# 导入交易记录，自动计算持仓和收益
python3 import_live_data.py --trades sample_trades.csv
```

**输出示例**：
```
✅ 成功导入 2 条交易记录
   买入: 2 笔
   卖出: 0 笔

🔄 自动计算持仓完成
   持仓品种: 2 个
   持仓总市值: 31050.00
   浮动盈亏: +0.00 (+0.00%)

📈 自动计算收益完成
   日期: 2026-02-05
   总市值: 31050.00
   日收益: +250.00 (+0.81%)
   累计收益: +0.00 (+0.00%)
```

### 3. 交互式录入

```bash
# 交互式录入，自动计算持仓和收益
python3 import_live_data.py --interactive
```

### 4. 手动触发计算（可选）

```bash
# 从交易记录重新计算持仓
python3 import_live_data.py --calc-positions

# 从持仓重新计算收益
python3 import_live_data.py --calc-pnl
```

## 计算逻辑说明

### 持仓计算

从交易记录自动计算当前持仓：

```python
for 每个股票代码:
    净持仓 = sum(买入股数) - sum(卖出股数)
    成本价 = sum(买入金额) / sum(买入股数)  # 加权平均
    实时价 = 拉取新浪实时行情
    盈亏 = (实时价 - 成本价) × 净持仓
```

**示例**：
- 2月5日买入招商银行 500股 @39.20
- 2月6日买入招商银行 500股 @39.50
- 2月7日卖出招商银行 300股 @40.00

计算结果：
- 持仓：500 + 500 - 300 = 700股
- 成本价：(500×39.20 + 500×39.50) / 1000 = 39.35
- 盈亏：(40.00 - 39.35) × 700 = +455元

### 收益计算

从持仓自动计算每日收益：

```python
今日总市值 = sum(每个持仓的市值)
今日收益 = 今日总市值 - 昨日总市值
今日收益率 = 今日收益 / 昨日总市值

累计成本 = sum(每个持仓的成本)
累计收益 = 今日总市值 - 累计成本
累计收益率 = 累计收益 / 累计成本
```

**示例**：
- 2月5日总市值：31,050元，累计收益：+250元
- 2月6日总市值：32,100元
- 计算：日收益 = 32,100 - 31,050 = +1,050元
- 日收益率 = 1,050 / 31,050 = +3.38%

## CSV 字段要求

### 交易记录（trades）

必填字段：

- `code`
- `trade_type`（BUY/SELL）
- `trade_date`（YYYY-MM-DD）
- `price`
- `shares`

可选字段：

- `name`
- `trade_time`
- `commission`
- `notes`

### 持仓（positions）

**自动计算**：从交易记录自动计算，无需手动导入

计算逻辑：
- 按股票代码分组
- 净持仓 = 买入总股数 - 卖出总股数
- 成本价 = 买入总成本 / 买入总股数（加权平均）
- 实时价格自动拉取（新浪接口）
- 自动计算市值、盈亏、盈亏率

如需手动导入，最小必填字段：

- `code`
- `shares`
- `cost_price`

可选字段：

- `name`
- `current_price`（自动拉取）
- `update_date`（自动填充）
- `update_time`（自动填充）

### 每日收益（daily_pnl）

**自动计算**：从持仓自动计算，无需手动导入

计算逻辑：
- 总市值 = 所有持仓市值之和
- 日收益 = 当日总市值 - 前一日总市值
- 日收益率 = 日收益 / 前一日总市值
- 累计收益 = 总市值 - 总成本
- 累计收益率 = 累计收益 / 总成本

如需手动导入，最小必填字段：

- `trade_date`
- `daily_pnl_pct`

可选字段：

- `total_value`
- `daily_pnl`
- `total_pnl`
- `total_pnl_pct`
# 日对账系统 - 使用指南

## 📋 系统概述

基于 SQLite 的日对账系统，用于验证量化回测与实盘交易的一致性。

### 核心功能
- ✅ 交易信号对账（买入/卖出信号）
- ✅ 持仓状态对账
- ✅ 收益数据对账
- ✅ 参数配置对账
- ✅ 自动生成对账报告
- ✅ 多种数据导入方式

---

## 🗄️ 数据库结构

### 回测数据表（已有）
```sql
-- 回测会话
backtest_sessions (session_id, start_time, end_time, status, metrics)

-- 回测交易
backtest_trades (trade_id, session_id, code, name, trade_type, trade_date, price, shares, pnl, exit_reason)

-- 回测持仓
backtest_positions (session_id, code, name, shares, cost_price, position_date)

-- 回测权益曲线
backtest_equity_curve (session_id, trade_date, total_value, daily_return)
```

### 实盘数据表（自动创建）
```sql
-- 实盘交易记录
live_trades (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT,
    trade_type TEXT NOT NULL,  -- 'BUY' or 'SELL'
    trade_date TEXT NOT NULL,
    trade_time TEXT,
    price REAL NOT NULL,
    shares INTEGER NOT NULL,
    amount REAL,
    commission REAL DEFAULT 0,
    status TEXT DEFAULT 'success',
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)

-- 实盘持仓
live_positions (
    code TEXT PRIMARY KEY,
    name TEXT,
    shares INTEGER NOT NULL,
    cost_price REAL NOT NULL,
    current_price REAL,
    market_value REAL,
    pnl REAL,
    pnl_pct REAL,
    update_date TEXT NOT NULL,
    update_time TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)

-- 实盘每日收益
live_daily_pnl (
    trade_date TEXT PRIMARY KEY,
    total_value REAL,
    daily_pnl REAL,
    daily_pnl_pct REAL,
    total_pnl REAL,
    total_pnl_pct REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
```

---

## 🚀 快速开始

### 1. 初始化实盘数据表

```bash
cd tests/momentum
python3 reconciliation/daily_reconciliation_db.py --init-tables
```

### 2. 导入实盘数据

#### 方式一：CSV 批量导入

```bash
# 生成示例CSV模板
python3 reconciliation/import_live_data.py --sample

# 编辑CSV文件后导入
python3 reconciliation/import_live_data.py --trades sample_trades.csv
python3 reconciliation/import_live_data.py --positions sample_positions.csv
python3 reconciliation/import_live_data.py --pnl sample_daily_pnl.csv
```

**CSV 格式示例：**

`sample_trades.csv`:
```csv
code,name,trade_type,trade_date,trade_time,price,shares,commission,notes
000001,平安银行,BUY,2025-02-05,09:35:00,11.50,1000,5.75,
600036,招商银行,SELL,2025-02-05,14:30:00,39.50,500,9.88,止盈
```

`sample_positions.csv`:
```csv
code,name,shares,cost_price,current_price,update_date,update_time
000001,平安银行,1000,11.50,11.60,2025-02-05,15:00:00
600519,贵州茅台,100,1680.00,1720.00,2025-02-05,15:00:00
```

`sample_daily_pnl.csv`:
```csv
trade_date,total_value,daily_pnl,daily_pnl_pct,total_pnl,total_pnl_pct
2025-02-05,102500.00,1200.00,1.18,2500.00,2.50
```

#### 方式二：交互式录入

```bash
python3 reconciliation/import_live_data.py --interactive
```

按提示输入交易信息，适合少量交易的手动录入。

### 3. 执行日对账

```bash
# 对账今天的数据
python3 reconciliation/daily_reconciliation_db.py

# 对账指定日期
python3 reconciliation/daily_reconciliation_db.py --date 2025-02-05

# 对账指定回测会话
python3 reconciliation/daily_reconciliation_db.py --date 2025-02-05 --session session_20250205_093000
```

### 4. 查看对账报告

```bash
# 报告保存在
ls -lh reconciliation/reports/reconciliation/*_reconciliation.md

# 查看最新报告
cat $(ls -t reconciliation/reports/reconciliation/*_reconciliation.md | head -1)
```

---

## ⚙️ 自动化部署

### 设置定时任务（Crontab）

```bash
# 编辑 crontab
crontab -e

# 添加以下行：每个工作日 10:00 执行对账
0 10 * * 1-5 /Users/admin/Documents/codeHub/adata-main/tests/momentum/reconciliation/auto_reconcile.sh

# 查看定时任务
crontab -l
```

### 自动化脚本说明

`auto_reconcile.sh` 自动执行：
1. （可选）运行回测
2. （可选）导入实盘数据
3. 执行日对账
4. 检查结果并记录日志
5. （可选）发送告警通知

---

## 📊 对账规则

### 1. 交易信号对账
- **完全匹配**：买入/卖出信号的股票代码必须完全一致
- **不匹配情况**：
  - 回测有信号，实盘无执行 → 需要调查
  - 实盘有交易，回测无信号 → 严重警报

### 2. 持仓对账
- **完全匹配**：持仓股票代码必须完全一致
- **允许差异**：数量和成本价可能因为分批建仓有小幅差异
- **不匹配情况**：
  - 持仓代码不一致 → 需要调查

### 3. 收益对账
- **允许偏差**：< 1% 视为正常（考虑滑点、手续费）
- **超出阈值**：> 1% 需要深入调查原因

### 4. 参数配置对账
- **必须完全一致**：所有关键参数必须相同
- **关键参数**：
  - FIXED_STOP_PCT（止损比例）
  - TAKE_PROFIT_PCT（止盈比例）
  - USE_ADAPTIVE_EXIT（动态退出）
  - MAX_TOTAL_PICKS（最大持仓数）
  - MAX_SECTOR_PICKS（板块限制）

---

## 🛠️ 常见问题

### Q1: 首次运行时显示"未找到回测数据"？

**A:** 这是正常的，需要先运行回测并保存到数据库：

```bash
cd tests/momentum
python3 backtest/run_backtest.py --save-to-db
```

### Q2: 如何清空实盘数据重新导入？

**A:** 使用 `--clear` 参数：

```bash
python3 reconciliation/import_live_data.py --trades new_trades.csv --clear
```

或直接操作数据库：

```bash
sqlite3 qlib_pro_v16.db "DELETE FROM live_trades WHERE trade_date='2025-02-05'"
```

### Q3: 对账发现差异怎么办？

**A:** 按以下步骤排查：

1. **查看对账报告**：了解具体差异类型
2. **检查实盘数据**：确认是否录入完整
3. **检查回测会话**：确认使用的是最新回测结果
4. **参数对账**：确认参数配置一致
5. **人工审查**：对于复杂情况，需要人工判断

### Q4: 如何对账历史数据？

**A:** 指定日期范围批量对账：

```bash
# 编写循环脚本
for date in 2025-02-01 2025-02-02 2025-02-03; do
    python3 reconciliation/daily_reconciliation_db.py --date $date
done
```

### Q5: 实盘数据从哪里获取？

**A:** 三种方式：

1. **券商交割单**：导出CSV后使用导入工具
2. **手动录入**：使用 `--interactive` 模式
3. **API对接**：开发券商API接口自动同步（待实现）

---

## 📈 最佳实践

### 1. 每日工作流程

**上午**（9:30 开盘前）：
- 查看前一日对账报告
- 确认今日交易计划

**盘中**（交易时段）：
- 记录实盘交易（实时或盘后）
- 记录异常情况和原因

**盘后**（15:00 收盘后）：
- 更新持仓数据
- 更新收益数据
- 执行日对账
- 审查对账报告

**晚间**（复盘时段）：
- 如有异常，深入分析
- 更新参数（如需调整）
- 记录到 Git tag 和 PARAMETER_CHANGELOG.md

### 2. 数据管理

```bash
# 定期备份数据库
cp qlib_pro_v16.db qlib_pro_v16_backup_$(date +%Y%m%d).db

# 定期归档对账报告
tar -czf reconciliation_reports_$(date +%Y%m).tar.gz reconciliation/reports/

# Git 版本控制
git add reconciliation/reports/
git commit -m "chore: 归档2月对账报告"
git tag -a reconcile-2025-02 -m "2月对账完成"
```

### 3. 告警机制

在 `auto_reconcile.sh` 中集成告警：

```bash
# 邮件告警
if grep -q "ALERT" "$LATEST_REPORT"; then
    mail -s "【对账异常】$(date +%Y-%m-%d)" your@email.com < "$LATEST_REPORT"
fi

# 微信/钉钉机器人告警
if grep -q "ALERT" "$LATEST_REPORT"; then
    curl -X POST 'https://your-webhook-url' \
         -H 'Content-Type: application/json' \
         -d "{\"text\":\"对账发现异常，请查看报告\"}"
fi
```

---

## 🔍 数据验证检查清单

### 导入数据前
- [ ] CSV 编码为 UTF-8
- [ ] 日期格式为 YYYY-MM-DD
- [ ] 交易类型为 BUY 或 SELL（大写）
- [ ] 价格和数量为正数
- [ ] 股票代码格式正确（6位数字）

### 对账前
- [ ] 回测已完成并保存到数据库
- [ ] 实盘交易已全部录入
- [ ] 持仓数据已更新到最新
- [ ] 收益数据已计算

### 对账后
- [ ] 交易信号对账通过
- [ ] 持仓对账通过
- [ ] 收益偏差在允许范围内
- [ ] 参数配置一致
- [ ] 报告已保存并备份

---

## 📝 示例输出

```
============================================================
[日对账] 开始对账 2025-02-05
[回测会话] session_20250205_093000
============================================================

1️⃣ 交易信号对账
   回测买入信号: ['000001', '600036']
   实盘买入信号: ['000001', '600036']
   回测卖出信号: ['000002']
   实盘卖出信号: ['000002']
   ✓ 交易信号完全一致

2️⃣ 持仓对账
   回测持仓: ['000001', '600036', '600519']
   实盘持仓: ['000001', '600036', '600519']
   ✓ 持仓完全一致

3️⃣ 收益对账
   回测收益: +1.25%
   实盘收益: +1.18%
   收益偏差: 0.56%
   ✓ 在允许范围内 (< 1%)

4️⃣ 参数配置对账
   ✓ 参数配置完全一致
      FIXED_STOP_PCT: 0.05
      TAKE_PROFIT_PCT: 0.1
      USE_ADAPTIVE_EXIT: True

============================================================
对账完成 - ✅ PASS - 完全对准
============================================================

📊 对账结果:
   交易信号: ✅
   持仓状态: ✅
   收益数据: ✅
   参数配置: ✅

📋 详细报告: ./reports/reconciliation/20250205_reconciliation.md
```

---

## 🔗 相关文档

- [量化工程标准规范](../../../.github/copilot/skills/quant-engineering-standards.md)
- [参数变更日志](../PARAMETER_CHANGELOG.md)
- [回测系统文档](../backtest/README.md)

---

## 💡 未来改进

- [ ] 券商API自动同步
- [ ] 告警通知系统（邮件/微信/钉钉）
- [ ] 对账差异自动分析
- [ ] 可视化对账报表
- [ ] 多账户对账支持
- [ ] 对账历史趋势分析

---

**维护者**: 量化团队  
**最后更新**: 2025-02-05
