# Momentum

Momentum 是一个面向 A 股尾盘扫描与短周期回测的量化策略项目，支持：

- `scan`：交易日尾盘选股
- `monitor`：盘中持仓诊断
- `backtest`：策略回测与统计分析

## 本地运行

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/test_backtest.py -q
python3 main.py --mode scan
python3 main.py --mode backtest --days 20 --no-report
```

## 飞书配置

当前通知模块发送的是飞书机器人标准消息体，因此请使用“自定义机器人”Webhook，而不是 `flow/api/trigger-webhook/...` 这一类流程触发地址。

正确的 URL 形式：

```text
https://open.feishu.cn/open-apis/bot/v2/hook/<your-token>
```

在 GitHub 仓库 `Settings -> Secrets and variables -> Actions` 中新增：

- `FEISHU_WEBHOOK_URL`：填入上面的机器人 webhook

如果没有配置这个 secret，Actions 仍然会继续执行，只是不发送通知。

## GitHub Actions

仓库内置了两个工作流：

- `momentum-scan.yml`：每个中国交易日 14:45 运行尾盘扫描
- `momentum-backtest.yml`：每周五收盘后运行 1 个月数据回测

为了方便刚推送后立即验证，这两个工作流都支持：

- `push`
- `workflow_dispatch`

其中：

- `schedule` 走生产参数
- `push` / `workflow_dispatch` 走更轻量的 smoke 参数，避免 CI 跑太久

## 时区说明

GitHub Actions 的 `cron` 使用 UTC：

- `14:45 Asia/Shanghai` 对应 `06:45 UTC`
- `15:05 Asia/Shanghai` 对应 `07:05 UTC`

工作流里已经额外加入了 `chinese-calendar` 判断，遇到中国法定非交易日会自动跳过计划任务。
