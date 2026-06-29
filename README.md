# Momentum — A 股量化交易系统

全自动 A 股短周期动量策略系统。覆盖盘前、竞价、盘中、盘后全时段，所有结果通过飞书推送。

## 功能矩阵

> 盘前 → 竞价 → 尾盘 → 盘后，全链条自动决策

| 时段 | 模块 | 输出 | 触发 |
|------|------|------|------|
| 08:30 | 盘前早报 | 7维开仓评分(±3)、4级建议、政策快讯 | GitHub cron |
| 09:25 | 竞价扫描 | 情绪判断、涨停板块主线(封单≥3确认) | cron-job.org |
| 14:44 | 尾盘选股 | 动量扫描、自适应止损、飞书推送 | cron-job.org 14:25→14:44 |
| 16:00 | 盘后归档 | 涨停/龙虎榜/机构动向/选股跟踪(D0-D3) | GitHub cron |
| 周五 | 周度回测 | 65天回测、多周期对比、飞书推送 | GitHub cron |

**盘前评分**: 外围(标普/纳指/A50/恒生) + 宏观(DXY/CNH/美债) + 政策 → ≥3积极 1-2谨慎 0~-1少动 ≤-2空仓

**竞价主线**: 涨停股 → 映射概念板块 → 统计集中度 → 主线确认/潜在主线

**选股跟踪**: 尾盘自动记录 → 盘后展示 D0(当日收盘)/D1/D2/D3 涨幅+盘中最高

## 架构

```
main.py          CLI 入口 (scan / backtest / monitor / optimize)
config.py        策略参数集中管理
core/            扫描引擎、因子计算、行业中性化
backtest/        回测模拟器、参数优化器、稳定性诊断
data/            数据获取 (新浪/腾讯/efinance/K线缓存降级)
risk/            退出规则、自适应止损
tools/           盘前早报、竞价扫描、盘后分析
```

## 本地运行

```bash
pip install -r requirements.txt
python main.py --mode scan                    # 尾盘选股
python main.py --mode backtest --days 65 --periods 5,3 --no-report  # 回测
python main.py --mode optimize --days 65      # 参数优化
```

## 飞书配置

使用飞书自定义机器人 Webhook：

```text
https://open.feishu.cn/open-apis/bot/v2/hook/<your-token>
```

本地配置 `config.local.json`（已 gitignore）：

```json
{
  "FEISHU_WEBHOOK_URL": "https://open.feishu.cn/open-apis/bot/v2/hook/<your-token>",
  "MOMENTUM_ENABLE_FEISHU_NOTIFICATION": true
}
```

GitHub Actions 中在 `Settings → Secrets → Actions` 设置 `FEISHU_WEBHOOK_URL`。

## 自动化工作流

| 工作流 | 主触发 | 时间 | 备用 |
|--------|--------|------|------|
| `pre-market.yml` | GitHub cron | 08:30 CST | workflow_dispatch |
| `auction-scan.yml` | cron-job.org | 09:25 CST | GitHub cron 09:20 |
| `momentum-scan.yml` | cron-job.org | 14:25→14:44 CST | GitHub cron 14:40 |
| `eod-analysis.yml` | GitHub cron | 16:00 CST | workflow_dispatch |
| `momentum-backtest.yml` | GitHub cron | 周五 15:05 CST | workflow_dispatch |

尾盘扫描采用两阶段执行：14:25 预取 K 线缓存 → 14:44 热缓存秒级扫描。`actions/cache@v4` 持久化 `qlib_pro_v16.db` 跨天复用。

## 外部 Cron 配置

[cron-job.org](https://console.cron-job.org)（免费）精准触发。共需配置两个：

**尾盘扫描** (`momentum-scan.yml`)：
- URL: `.../momentum-scan.yml/dispatches`, Method: POST
- Body: `{"ref":"main","inputs":{"run_mode":"full"}}`
- Cron: `25 14 * * 1-5`, 时区 Asia/Shanghai

**竞价扫描** (`auction-scan.yml`)：
- URL: `.../auction-scan.yml/dispatches`, Method: POST
- Body: `{"ref":"main"}`
- Cron: `25 1 * * 1-5`, 时区 Asia/Shanghai

Headers 统一：`Authorization: Bearer <GH_PAT>`, `Accept: application/vnd.github+json`, `X-GitHub-Api-Version: 2022-11-28`, `Content-Type: application/json`。

Token 需要 `workflow` scope。

## 参数优化

65 天网格搜索：

| 参数 | 优化前 | 优化后 |
|------|--------|--------|
| MAX_TOTAL_PICKS | 3 | 1 |
| MAX_SECTOR_PICKS | 1 | 2 |
| HOLD_PERIOD | 5 | 5 |
| USE_ADAPTIVE_EXIT | True | True |

收益 +11.4%（8.68% → 9.67%）。单只持仓回撤从 6.61% 增至 15.25%。

## 常见问题

| 问题 | 原因 | 解决 |
|------|------|------|
| 飞书推 2 只而非 1 只 | scanner.py `max(2,...)` 硬编码 | 已修复为 `max(1,...)` |
| 盘后扫描无数据 | 实时 API 收盘失效 | K 线缓存降级自动接管 |
| Actions 排队延迟 | GitHub 调度器不保证准时 | cron-job.org 外部触发 |
| 回测 15 分钟+ | 全量 K 线从头拉取 | actions/cache 预热 |

## 时区

GitHub cron 使用 UTC。`chinese-calendar` 自动跳过非交易日。
