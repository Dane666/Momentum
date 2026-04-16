# 实盘数据导入自动计算功能演示

## 场景：完整的交易流程

### 第一天（2026-02-05）：建仓

**创建交易记录 CSV**：

```csv
code,name,trade_type,trade_date,trade_time,price,shares,commission,notes
000001,平安银行,BUY,2026-02-05,09:35:00,11.50,1000,5.75,首次建仓
600036,招商银行,BUY,2026-02-05,10:00:00,39.20,500,9.80,首次建仓
```

**导入并自动计算**：

```bash
python3 import_live_data.py --trades day1_trades.csv
```

**系统自动执行**：
1. ✅ 导入2笔交易记录
2. 🔄 自动计算持仓（2个品种）
3. 📈 自动计算收益

**结果**：
- 持仓：平安银行 1000股 @11.50，招商银行 500股 @39.20
- 总市值：31,100元
- 累计收益：0元（刚建仓）

---

### 第二天（2026-02-06）：加仓

**新增交易记录**：

```csv
code,name,trade_type,trade_date,trade_time,price,shares,commission,notes
600036,招商银行,BUY,2026-02-06,09:45:00,39.50,500,9.88,加仓
```

**导入并自动计算**：

```bash
python3 import_live_data.py --trades day2_trades.csv
```

**系统自动执行**：
1. ✅ 导入1笔交易记录
2. 🔄 自动重新计算所有持仓（招商银行持仓合并）
3. 📈 自动计算今日收益（对比昨日市值）

**结果**：
- 持仓更新：招商银行 1000股 @39.35（加权平均成本）
- 总市值：50,850元
- 日收益：+19,750元（新增资金）

---

### 第三天（2026-02-07）：止盈

**新增交易记录**：

```csv
code,name,trade_type,trade_date,trade_time,price,shares,commission,notes
600036,招商银行,SELL,2026-02-07,14:30:00,40.00,500,10.00,止盈退出
```

**导入并自动计算**：

```bash
python3 import_live_data.py --trades day3_trades.csv
```

**系统自动执行**：
1. ✅ 导入1笔卖出记录
2. 🔄 自动重新计算持仓（招商银行减少500股）
3. 📈 自动计算今日收益

**结果**：
- 持仓更新：招商银行 500股 @39.35
- 卖出盈利：(40.00 - 39.35) × 500 = +325元
- 总市值更新（剩余持仓）

---

## 手动触发重新计算

如果修改了历史交易记录，可以手动重新计算：

```bash
# 1. 从所有交易记录重新计算持仓
python3 import_live_data.py --calc-positions

# 2. 从持仓重新计算收益
python3 import_live_data.py --calc-pnl
```

---

## 数据查询

```bash
# 查看所有交易记录
sqlite3 ../qlib_pro_v16.db "SELECT * FROM live_trades ORDER BY trade_date, trade_time"

# 查看当前持仓
sqlite3 ../qlib_pro_v16.db "SELECT code, name, shares, cost_price, current_price, pnl FROM live_positions"

# 查看每日收益
sqlite3 ../qlib_pro_v16.db "SELECT * FROM live_daily_pnl ORDER BY trade_date"
```

---

## 核心优势

1. **单一数据源**：只需维护交易记录
2. **自动化**：导入即计算，无需手动干预
3. **一致性**：持仓和收益始终基于交易记录计算
4. **可追溯**：所有计算逻辑透明可查
5. **灵活性**：支持任意时间重新计算

---

## 注意事项

⚠️ **成本价计算**：当前使用简化的加权平均法
- 买入成本 = 所有买入的总金额 / 总股数
- 不考虑卖出后的成本调整（FIFO/LIFO）

⚠️ **首日收益**：第一天没有历史对比基准
- 日收益 = 累计收益（等于浮动盈亏）
- 从第二天开始才有真正的"日收益"概念

⚠️ **实时价格**：使用新浪接口拉取
- 如果拉取失败，会回退使用成本价
- 可能存在15分钟延迟
