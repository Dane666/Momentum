# CLAUDE.md — Momentum 量化策略

## 项目概述

A 股尾盘动量选股 + 短周期回测系统。每日 14:44 扫描全市场，筛选 1-3 只动量最强的股票，持仓 5 天，自适应止盈止损退出。飞书推送结果。

## 核心架构

```
main.py          → CLI 入口，模式分发
config.py        → 所有阈值集中管理
core/            → 扫描引擎、因子计算、行业中性化
backtest/        → 回测模拟器、参数优化器、稳定性分析
data/            → 数据获取 (新浪/腾讯/efinance/K线缓存降级)
risk/            → 退出规则、自适应止损
tools/           → 盘前早报、竞价扫描、盘后分析
```

## 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| MAX_TOTAL_PICKS | 1 | 选股上限 |
| HOLD_PERIOD_DEFAULT | 5 | 持仓天数 |
| MIN_AMOUNT | 2亿 | 成交额下限 |
| USE_ADAPTIVE_EXIT | True | 自适应止损 |

## 工作流

| 文件 | 触发 | 时间 |
|------|------|------|
| momentum-scan.yml | cron-job.org | 14:25→14:45 (含低位绩优股扫描) |
| pre-market.yml | schedule | 08:30 |
| auction-scan.yml | cron-job.org | 09:25 |
| eod-analysis.yml | schedule | 16:00 |
| momentum-backtest.yml | schedule/Fri | 15:05 |

## 本地开发

```bash
python main.py --mode scan
python main.py --mode backtest --days 65 --periods 5,3 --no-report
git push origin main
```

## 常见问题

**飞书推送 2 只**: 检查 scanner.py:240 `base_picks = max(1, cfg.MAX_TOTAL_PICKS)`
**盘后无数据**: K 线缓存降级自动接管，需 qlib_pro_v16.db 已预热
**Actions 日志**: `gh run view --repo Dane666/Momentum --log <ID>`
