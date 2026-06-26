# Momentum

Momentum 是一个面向 A 股尾盘扫描与短周期回测的量化策略项目，支持：

- `scan`：交易日尾盘选股
- `monitor`：盘中持仓诊断
- `backtest`：策略回测与统计分析

## 本地运行

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/test_backtest.py tests/test_fetcher.py tests/test_fetcher_local_fallback.py -q
python3 main.py --mode scan
python3 main.py --mode backtest --days 20 --no-report
```

## 飞书配置

当前通知模块发送的是飞书机器人标准消息体，因此请使用“自定义机器人”Webhook，而不是 `flow/api/trigger-webhook/...` 这一类流程触发地址。

正确的 URL 形式：

```text
https://open.feishu.cn/open-apis/bot/v2/hook/<your-token>
```

### 本地配置

项目根目录支持本地配置文件：

```text
config.local.json
```

可以参考：

```text
config.local.example.json
```

示例内容：

```json
{
  "FEISHU_WEBHOOK_URL": "https://open.feishu.cn/open-apis/bot/v2/hook/<your-token>",
  "MOMENTUM_ENABLE_FEISHU_NOTIFICATION": true
}
```

`config.local.json` 已加入 `.gitignore`，适合保存本机 webhook，不需要额外设置环境变量。

在 GitHub 仓库 `Settings -> Secrets and variables -> Actions` 中新增：

- `FEISHU_WEBHOOK_URL`：填入上面的机器人 webhook

如果没有配置这个 secret，Actions 仍然会继续执行，只是不发送通知。

## GitHub Actions

仓库内置了两个工作流：

- `momentum-scan.yml`：每交易日 14:25 预取数据，14:44 尾盘扫描
- `momentum-backtest.yml`：每周五收盘后运行回测

### 触发方式

| 触发方式 | 时间 | 精度 |
|---------|------|------|
| **cron-job.org**（主） | 14:25 CST | ±1s |
| GitHub schedule（备用） | 14:40 CST | ±15min 排队 |
| workflow_dispatch | 手动 | 即时 |
| push | 即时 | 仅跑单元测试 |

### scan 两步执行

```
14:25  外部 cron 精准触发 → 预取 200 只 K 线 → SQLite 缓存
14:35  Sleep 等待市场窗口
14:44  启动扫描 → 缓存全命中 → 秒级完成 → 飞书推送 📱
```

`actions/cache@v4` 持久化 `qlib_pro_v16.db`，每日预热加速次日.

## 外部 Cron 配置

用 [cron-job.org](https://console.cron-job.org)（免费）精准触发：

| 字段 | 值 |
|------|-----|
| URL | `https://api.github.com/repos/Dane666/Momentum/actions/workflows/momentum-scan.yml/dispatches` |
| Method | POST |
| Headers | `Authorization: Bearer <GH_PAT>` / `Accept: application/vnd.github+json` / `X-GitHub-Api-Version: 2022-11-28` / `Content-Type: application/json` |
| Body | `{"ref":"main","inputs":{"run_mode":"full"}}` |
| Cron | `25 14 * * 1-5`，时区 Asia/Shanghai |

需要 GitHub PAT，scope 最少 `workflow`。

## 参数优化

65 天网格搜索，以收益率为目标：

| 参数 | 优化前 | 优化后 |
|------|--------|--------|
| MAX_TOTAL_PICKS | 3 | **1** |
| MAX_SECTOR_PICKS | 1 | 2 |
| use_adaptive_exit | True | True |

收益 8.68% → 9.67%（+11.4%）。单只持仓回撤增大（6.61% → 15.25%）。

## 时区

GitHub cron 使用 UTC：`14:40 CST` = `06:40 UTC`。`chinese-calendar` 自动跳过非交易日。
