# 股票数据 ETL 脚本

## 功能概述

自动化处理 iPhone 截图中的股票持仓和成交记录：
1. 轮询飞书多维表格获取待处理记录
2. 下载截图并使用 PaddleOCR 识别文字
3. 调用本地 Ollama (qwen2.5) 将 OCR 文本解析为结构化 JSON
4. 存入本地 SQLite 数据库
5. 更新飞书表格状态

## 环境要求

- Python 3.10+
- Ollama 已安装并运行 `qwen2.5` 模型
- 飞书开放平台应用凭证

## 安装依赖

```bash
# 创建虚拟环境（推荐）
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

## 配置说明

在 `main.py` 顶部修改以下配置：

```python
# 飞书应用凭证
FEISHU_APP_ID = "cli_a9fa070bedf89bcd"          # 飞书应用 App ID
FEISHU_APP_SECRET = "obLbml0T7pPy10rI3fVDVeNGzjUQM4lh"       # 飞书应用 App Secret


# 多维表格信息
FEISHU_APP_TOKEN = "Yr7QbMWMoaV48QsyXiOcNgKznDg"         # 多维表格 App Token
FEISHU_TABLE_ID = "tblcRUyyXxpf8S5f"           # 数据表 Table ID

# Ollama 模型
OLLAMA_MODEL = "qwen2.5"                    # 可更换为其他模型
```

### 获取飞书凭证

1. 登录 [飞书开放平台](https://open.feishu.cn/)
2. 创建企业自建应用，获取 App ID 和 App Secret
3. 添加权限：`bitable:app`, `drive:drive:readonly`
4. 从多维表格 URL 获取 App Token: `https://xxx.feishu.cn/base/{app_token}`
5. 在多维表格中获取 Table ID

## 飞书多维表格结构

创建名为 `Stock_Sync_Queue` 的数据表，包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| 截图类型 | 多行文本 | 值为 "持仓详情" 或 "当日成交" |
| 处理状态 | 多行文本 | 初始 "待处理"，处理后更新为 "已完成" 或 "识别失败" |
| 原始截图 | 附件 | 上传的截图图片 |
| 处理日志 | 多行文本 | 回写 JSON 结果或错误信息 |
| 上传时间 | 日期 | 自动生成的上传时间 |

## 运行

```bash
# 确保 Ollama 正在运行
ollama serve

# 在另一个终端运行脚本
python main.py
```

脚本会每 10 秒轮询一次飞书表格。

## 数据库结构

SQLite 数据库 `stock_data.db` 包含两张表：

### portfolio_snapshot (持仓快照)
- id, snapshot_date, stock_name, market_value, holding_qty, floating_pnl, today_pnl, created_at

### daily_transactions (成交记录)
- id, trade_time, stock_name, action, price, volume, amount, created_at
- 唯一索引：(trade_time, stock_name, action, volume)

## 日志

运行日志同时输出到控制台和 `etl.log` 文件。
