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

- `momentum-scan.yml`：每个中国交易日 14:45 运行尾盘扫描
- `momentum-backtest.yml`：每周五收盘后运行 1 个月数据回测

为了方便刚推送后立即验证，这两个工作流都支持：

- `push`
- `workflow_dispatch`

其中：

- `momentum-scan.yml`：`push` 默认运行全量 scan，并开启飞书通知
- `workflow_dispatch`：默认 `full`，也支持手动切回 `smoke`
- `schedule`：继续走生产参数
- `momentum-backtest.yml`：默认仍保留手动 `full` / `smoke` 两种模式

## 时区说明

GitHub Actions 的 `cron` 使用 UTC：

- `14:45 Asia/Shanghai` 对应 `06:45 UTC`
- `15:05 Asia/Shanghai` 对应 `07:05 UTC`

工作流里已经额外加入了 `chinese-calendar` 判断，遇到中国法定非交易日会自动跳过计划任务。
