#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据自动化 ETL 脚本
========================

功能：
1. 轮询飞书多维表格，获取待处理的截图记录
2. 下载图片并使用 PaddleOCR 进行文字识别
3. 调用本地 Ollama (qwen2.5) 将 OCR 结果解析为结构化 JSON
4. 将数据存入本地 SQLite 数据库
5. 回写飞书表格状态

依赖：
- paddleocr: 本地 OCR 识别
- ollama: 本地 LLM 调用
- requests: HTTP 请求

使用前请配置：
- FEISHU_APP_ID: 飞书应用 ID
- FEISHU_APP_SECRET: 飞书应用密钥
- FEISHU_APP_TOKEN: 多维表格的 App Token
- FEISHU_TABLE_ID: 数据表的 Table ID
"""

import os
import sys
import json
import time
import sqlite3
import tempfile
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

import requests
import ollama
import base64

# ===================== 配置区域 =====================
# 请替换为你的飞书应用凭证
FEISHU_APP_ID = "cli_a9fa070bedf89bcd"          # 飞书应用 App ID
FEISHU_APP_SECRET = "obLbml0T7pPy10rI3fVDVeNGzjUQM4lh"       # 飞书应用 App Secret

# 多维表格信息
# App Token 可从多维表格 URL 中获取: https://xxx.feishu.cn/base/{app_token}
FEISHU_APP_TOKEN = "Yr7QbMWMoaV48QsyXiOcNgKznDg"         # 多维表格 App Token
FEISHU_TABLE_ID = "tblcRUyyXxpf8S5f"           # 数据表 Table ID

# 本地 Ollama 视觉模型配置
# OLLAMA_VISION_MODEL = "minicpm-v"            # 视觉模型，直接看图提取数据
OLLAMA_VISION_MODEL = "minicpm-v"  # 更换为更适合文档理解的模型     

# 数据库配置
DB_PATH = Path(__file__).parent / "stock_data.db"

# 轮询间隔（秒）
POLL_INTERVAL = 10

# ===================== 日志配置 =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "etl.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# ===================== Vision LLM Prompts =====================
# 视觉模型直接看图，无需 OCR 文本占位符
PROMPT_HOLDINGS_VISION = """识别这张证券APP持仓截图中的所有股票/ETF持仓。

需提取字段（所有数值不要带任何符号如%、¥等）：
- stock_name: 股票/ETF名称（字符串）
- market_value: 市值（纯数字）
- position_pct: 个股仓位百分比（纯数字，如5.2代表5.2%，如果没有则填0）
- cost_price: 成本价（纯数字）
- current_price: 现价（纯数字）
- floating_pnl: 浮动盈亏（纯数字，可为负）
- today_pnl: 今日盈亏（纯数字，可为负）

【识别区域说明】：
1. 请重点关注图片中下部的【持仓列表】区域。
2. 通常有一个表头行（包含名称/市值/盈亏等），请识别表头下方的每一行数据。
3. **忽略**图片顶部的总资产、总盈亏等汇总区域的数字，那些不是个股持仓。


【严格要求】:
1. 必须返回有效的 JSON 数组格式
2. 不要有任何解释文字、说明或代码块标记
3. 尽可能识别图片中的所有记录
4. 如果某字段看不清，请根据上下文合理推断或留空，尽量不要丢弃整条记录

【重要】只输出 JSON 数组，不要有任何解释文字、不要有```json标记。
示例输出：
[{"stock_name": "金龙羽", "market_value": 10191.00, "position_pct": 5.2, "cost_price": 33.98, "current_price": 33.97, "floating_pnl": -226.00, "today_pnl": -222.00}]
"""

PROMPT_TRANSACTIONS_VISION = """这是一张证券APP的成交记录截图。请识别并提取所有清晰可见的交易记录。

需提取的字段（必需，不得为空）：
- trade_time: 成交时间（HH:mm:ss格式，如14:24:02）
- stock_name: 股票或基金名称（字符串）
- action: 操作类型（买入/卖出/融券/融券买入等）
- price: 成交价格（数字，如26.73）
- volume: 成交数量（整数，如400）
- amount: 成交金额（数字，如10692.00）

【严格要求】:
1. 必须返回有效的 JSON 数组格式
2. 不要有任何解释文字、说明或代码块标记
3. 尽可能识别图片中的所有记录
4. 如果某字段看不清，请根据上下文合理推断或留空，尽量不要丢弃整条记录

示例输出（必须是这种格式）：
[{"trade_time": "14:24:02", "stock_name": "国邦医药", "action": "买入", "price": 26.73, "volume": 400, "amount": 10692.00}]
"""


class FeishuClient:
    """
    飞书开放平台 API 客户端
    
    实现功能：
    - 获取并缓存 tenant_access_token（自动刷新）
    - 获取待处理记录
    - 下载附件图片
    - 更新记录状态
    """
    
    BASE_URL = "https://open.feishu.cn/open-apis"
    
    def __init__(self, app_id: str, app_secret: str, app_token: str, table_id: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.table_id = table_id
        
        # Token 缓存
        self._access_token: Optional[str] = None
        self._token_expire_time: float = 0
    
    def _get_tenant_access_token(self) -> str:
        """
        获取 tenant_access_token，带缓存机制
        
        Token 有效期为 2 小时，提前 5 分钟刷新
        """
        current_time = time.time()
        
        # 检查缓存是否有效（提前 5 分钟刷新）
        if self._access_token and current_time < self._token_expire_time - 300:
            return self._access_token
        
        # 请求新 Token
        url = f"{self.BASE_URL}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != 0:
                raise Exception(f"获取 Token 失败: {data.get('msg')}")
            
            self._access_token = data["tenant_access_token"]
            # Token 有效期 2 小时（7200秒）
            self._token_expire_time = current_time + data.get("expire", 7200)
            
            logger.info("成功获取飞书 tenant_access_token")
            return self._access_token
            
        except requests.RequestException as e:
            logger.error(f"请求飞书 Token API 失败: {e}")
            raise
    
    def _get_headers(self) -> Dict[str, str]:
        """获取带认证的请求头"""
        return {
            "Authorization": f"Bearer {self._get_tenant_access_token()}",
            "Content-Type": "application/json; charset=utf-8"
        }
    
    def get_pending_records(self) -> List[Dict[str, Any]]:
        """
        获取所有待处理的记录
        
        筛选条件：处理状态 = "待处理"
        
        Returns:
            记录列表，每条记录包含 record_id, 截图类型, 原始截图 等字段
        """
        # 使用 list API 获取所有记录，然后客户端过滤
        url = f"{self.BASE_URL}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        
        params = {
            "page_size": 100
        }
        
        try:
            resp = requests.get(url, headers=self._get_headers(), params=params, timeout=30)
            
            # 打印详细错误信息以便调试
            if resp.status_code != 200:
                logger.error(f"API 响应状态码: {resp.status_code}")
                logger.error(f"API 响应内容: {resp.text[:500]}")
            
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != 0:
                logger.error(f"获取记录失败: {data.get('msg')}")
                return []
            
            all_records = data.get("data", {}).get("items", [])
            
            # 客户端过滤：只保留待处理的记录
            pending_records = []
            for record in all_records:
                fields = record.get("fields", {})
                status = fields.get("处理状态", "")
                if status == "待处理":
                    pending_records.append(record)
            
            if pending_records:
                logger.info(f"获取到 {len(pending_records)} 条待处理记录 (共 {len(all_records)} 条)")
            
            return pending_records
            
        except requests.RequestException as e:
            logger.error(f"请求飞书记录列表失败: {e}")
            return []
    
    def download_image(self, file_token: str) -> Optional[bytes]:
        """
        下载附件图片
        
        Args:
            file_token: 附件的 file_token
            
        Returns:
            图片二进制数据，失败返回 None
        """
        url = f"{self.BASE_URL}/drive/v1/medias/{file_token}/download"
        
        try:
            resp = requests.get(url, headers=self._get_headers(), timeout=60)
            resp.raise_for_status()
            
            logger.info(f"成功下载图片: {file_token}")
            return resp.content
            
        except requests.RequestException as e:
            logger.error(f"下载图片失败 [{file_token}]: {e}")
            return None
    
    def update_record(self, record_id: str, status: str, log_text: str) -> bool:
        """
        更新记录状态和日志
        
        Args:
            record_id: 记录 ID
            status: 新状态（"已完成" 或 "识别失败"）
            log_text: 日志内容（JSON 结果或错误信息）
            
        Returns:
            是否更新成功
        """
        url = f"{self.BASE_URL}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"
        
        payload = {
            "fields": {
                "处理状态": status,
                "处理日志": log_text[:65535]  # 限制长度，避免超出字段限制
            }
        }
        
        try:
            resp = requests.put(url, headers=self._get_headers(), json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != 0:
                logger.error(f"更新记录失败: {data.get('msg')}")
                return False
            
            logger.info(f"成功更新记录 [{record_id}] 状态为: {status}")
            return True
            
        except requests.RequestException as e:
            logger.error(f"更新记录请求失败 [{record_id}]: {e}")
            return False


class AIProcessor:
    """
    AI 处理器 (Vision LLM 版本)
    
    使用 minicpm-v 视觉模型直接从图片提取结构化数据，无需 OCR
    """
    
    def __init__(self, model_name: str = OLLAMA_VISION_MODEL):
        self.model_name = model_name
        logger.info(f"使用视觉模型: {self.model_name}")
    
    def _encode_image_to_base64(self, image_path: str) -> str:
        """
        将图片编码为 base64 字符串
        """
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    
    def _parse_json_response(self, content: str, screenshot_type: str) -> List[Dict[str, Any]]:
        """
        解析 LLM 响应中的 JSON，支持多种格式和错误恢复
        """
        import re
        
        # 验证内容不为空
        if not content or not content.strip():
            logger.error("LLM 响应内容为空")
            raise ValueError("LLM 返回空响应")
        
        # 记录原始响应的长度和预览
        logger.debug(f"原始响应长度: {len(content)} 字符")
        logger.debug(f"原始响应预览: {content[:200]}...")
        
        # 步骤 1: 清理 Markdown 代码块
        cleaned_content = content
        if content.startswith("```"):
            lines = content.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned_content = "\n".join(lines).strip()
            logger.debug("已移除 Markdown 代码块")
        
        # 步骤 2: 尝试直接解析
        try:
            result = json.loads(cleaned_content)
            logger.info(f"✓ 成功直接解析 JSON，共 {len(result) if isinstance(result, list) else 1} 条记录")
            return self._validate_records(result, screenshot_type)
        except json.JSONDecodeError as e:
            logger.debug(f"直接解析失败 (位置 {e.pos}): {e.msg}")
        
        # 步骤 3: 尝试单引号替换（最常见的问题）
        try:
            fixed_content = cleaned_content.replace("'", '"')
            result = json.loads(fixed_content)
            logger.info(f"✓ 单引号替换后成功解析，共 {len(result) if isinstance(result, list) else 1} 条记录")
            return self._validate_records(result, screenshot_type)
        except json.JSONDecodeError as e:
            logger.debug(f"单引号替换后仍失败: {e.msg}")
        
        # 步骤 4: 使用正则提取 JSON 数组
        json_array_match = re.search(r'\[[\s\S]*\]', cleaned_content)
        if json_array_match:
            extracted = json_array_match.group()
            logger.debug(f"通过正则提取到 JSON 数组，长度: {len(extracted)}")
            
            # 尝试直接解析提取的内容
            try:
                result = json.loads(extracted)
                logger.info(f"✓ 正则提取后直接解析成功，共 {len(result)} 条记录")
                return self._validate_records(result, screenshot_type)
            except json.JSONDecodeError:
                logger.debug(f"正则提取的内容直接解析失败，尝试单引号替换...")
            
            # 尝试在提取后的内容上进行单引号替换
            try:
                fixed_extracted = extracted.replace("'", '"')
                result = json.loads(fixed_extracted)
                logger.info(f"✓ 正则提取 + 单引号替换后成功，共 {len(result)} 条记录")
                return self._validate_records(result, screenshot_type)
            except json.JSONDecodeError as e2:
                logger.debug(f"正则提取 + 替换仍失败 (位置 {e2.pos}): {e2.msg}")
        
        # 步骤 5: 尝试提取单个 JSON 对象（针对返回单个记录的情况）
        json_obj_match = re.search(r'\{[\s\S]*\}', cleaned_content)
        if json_obj_match:
            extracted = json_obj_match.group()
            logger.debug(f"通过正则提取到 JSON 对象")
            
            try:
                result = json.loads(extracted)
                logger.info(f"✓ 单个 JSON 对象解析成功")
                return self._validate_records([result], screenshot_type)
            except json.JSONDecodeError:
                try:
                    fixed_extracted = extracted.replace("'", '"')
                    result = json.loads(fixed_extracted)
                    logger.info(f"✓ 单个对象 + 单引号替换成功")
                    return self._validate_records([result], screenshot_type)
                except json.JSONDecodeError as e3:
                    logger.debug(f"单个对象解析也失败: {e3.msg}")
        
        # 步骤 6: 所有方法都失败，记录完整响应以便调试
        logger.error(f"所有 JSON 解析方法都失败")
        logger.error(f"完整响应内容: {cleaned_content[:500]}")
        raise ValueError(f"无法从响应中提取有效 JSON (长度: {len(cleaned_content)})")
    
    def _optimize_image(self, image_path: str, max_size: int = 2048) -> str:
        """
        如果图片过大，调整大小以加快处理并避免空响应
        """
        try:
            from PIL import Image
            import os
            
            with Image.open(image_path) as img:
                width, height = img.size
                
                # 如果长边不超过限制，直接返回原图
                if max(width, height) <= max_size:
                    return image_path
                
                # 计算缩放比例
                ratio = max_size / max(width, height)
                new_size = (int(width * ratio), int(height * ratio))
                
                # 调整大小
                img_resized = img.resize(new_size, Image.Resampling.LANCZOS)
                
                # 保存为新的临时文件
                dir_name = os.path.dirname(image_path)
                base_name = os.path.basename(image_path)
                new_path = os.path.join(dir_name, f"optimized_{base_name}")
                
                # 保存 (转换为RGB以防RGBA兼容性问题)
                if img_resized.mode in ("RGBA", "P"):
                    img_resized = img_resized.convert("RGB")
                    
                img_resized.save(new_path, quality=85)
                logger.info(f"图片已压缩: {width}x{height} -> {new_size[0]}x{new_size[1]}")
                return new_path
                
        except ImportError:
            logger.warning("未安装 PIL (Pillow)，跳过图片优化")
            return image_path
        except Exception as e:
            logger.warning(f"图片优化失败: {e}，将使用原图")
            return image_path

    def _validate_records(self, result: Any, screenshot_type: str) -> List[Dict[str, Any]]:
        """
        验证和过滤解析得到的记录
        """
        if not isinstance(result, list):
            result = [result] if result else []
        
        logger.info(f"原始记录数: {len(result)}")
        
        # 验证必要字段
        if screenshot_type == "当日成交":
            required_fields = ["trade_time", "stock_name", "action"]
        else:
            required_fields = ["stock_name"]
        
        valid_records = []
        for i, record in enumerate(result):
            if not isinstance(record, dict):
                logger.warning(f"记录 {i} 不是字典类型: {type(record)}")
                continue
            
            missing = [f for f in required_fields if f not in record or not record[f]]
            if missing:
                logger.debug(f"记录 {i} 缺少必要字段 {missing}")
            else:
                valid_records.append(record)
        
        logger.info(f"有效记录数: {len(valid_records)}/{len(result)}")
        return valid_records
    
    def process_image(self, image_path: str, screenshot_type: str, max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        使用视觉模型直接从图片提取结构化数据
        
        Args:
            image_path: 图片路径
            screenshot_type: 截图类型（"持仓详情" 或 "当日成交"）
            max_retries: 最大重试次数（默认 3）
            
        Returns:
            结构化数据列表
        """
        import re
        
        # 根据截图类型选择 Prompt
        if screenshot_type == "持仓详情":
            prompt = PROMPT_HOLDINGS_VISION
        elif screenshot_type == "当日成交":
            prompt = PROMPT_TRANSACTIONS_VISION
        else:
            raise ValueError(f"未知的截图类型: {screenshot_type}")
        
        last_error = None
        
        for retry_count in range(max_retries):
            try:
                logger.info(f"正在使用视觉模型 ({self.model_name}) 分析图片... (尝试 {retry_count + 1}/{max_retries})")
                logger.debug(f"使用提示词长度: {len(prompt)} 字符")
                
                # 优化图片大小 (如果过大)
                optimized_image_path = self._optimize_image(image_path)
                
                # 调用 Ollama 视觉模型
                response = ollama.chat(
                    model=self.model_name,
                    messages=[{
                        "role": "user",
                        "content": prompt,
                        "images": [optimized_image_path]
                    }],
                    options={
                        "temperature": 0.2,  # 稍微提高一点温度避免死循环
                        "num_predict": 4096,
                        "num_ctx": 4096
                    },
                    keep_alive="5m" # 保持模型加载
                )
                
                content = response["message"]["content"].strip()
                logger.info(f"视觉模型响应长度: {len(content)} 字符")
                
                # 响应验证：检查是否为空
                if not content:
                    logger.warning(f"【{screenshot_type}】视觉模型返回空响应，准备重试...")
                    last_error = "模型返回空响应"
                    if retry_count < max_retries - 1:
                        time.sleep(2)  # 等待后重试
                        continue
                    else:
                        # 记录诊断信息
                        logger.error(f"【{screenshot_type}】多次重试后仍返回空")
                        logger.error(f"  提示词类型: {screenshot_type}")
                        logger.error(f"  提示词长度: {len(prompt)} 字符")
                        raise ValueError("视觉模型在多次尝试后仍返回空响应")
                
                # 记录完整响应（用于诊断）
                if len(content) < 500:
                    logger.debug(f"完整响应: {content}")
                else:
                    logger.debug(f"响应预览: {content[:300]}...{content[-200:]}")
                
                # 解析 JSON 响应
                valid_records = self._parse_json_response(content, screenshot_type)
                
                logger.info(f"✓ 解析完成，共 {len(valid_records)} 条有效记录")
                return valid_records
                
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 解析失败 (尝试 {retry_count + 1}/{max_retries}): {e}")
                logger.debug(f"  错误位置: 第 {e.pos} 字符")
                last_error = f"JSON 解析失败: {str(e)}"
                if retry_count < max_retries - 1:
                    logger.info("准备重试...")
                    time.sleep(2)  # 等待后重试
                    continue
                else:
                    raise
            except Exception as e:
                import traceback
                logger.warning(f"视觉模型处理异常 (尝试 {retry_count + 1}/{max_retries}): {e}")
                last_error = str(e)
                if retry_count < max_retries - 1:
                    logger.info("准备重试...")
                    time.sleep(2)  # 等待后重试
                    continue
                else:
                    logger.error(f"错误堆栈: {traceback.format_exc()}")
                    raise
        
        # 如果所有重试都失败
        raise Exception(f"视觉模型在 {max_retries} 次尝试后仍然失败: {last_error}")


class DatabaseManager:
    """
    SQLite 数据库管理器
    
    负责数据库初始化和数据存储
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        except sqlite3.OperationalError as e:
            logger.error(f"数据库连接失败: {e}")
            if "locked" in str(e).lower():
                logger.error("💡 数据库被锁定，请检查是否有其他进程在使用数据库")
            raise
        cursor = conn.cursor()
        
        # 创建持仓快照表 (增强版，使用 trade_date)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date DATE NOT NULL,
                stock_name TEXT NOT NULL,
                market_value REAL,
                position_pct REAL,
                cost_price REAL,
                current_price REAL,
                floating_pnl REAL,
                today_pnl REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 检查是否需要迁移 snapshot_date -> trade_date
        try:
            cursor.execute("PRAGMA table_info(portfolio_snapshot)")
            columns = [info[1] for info in cursor.fetchall()]
            if 'snapshot_date' in columns and 'trade_date' not in columns:
                logger.info("正在迁移数据库: snapshot_date -> trade_date")
                cursor.execute("ALTER TABLE portfolio_snapshot RENAME COLUMN snapshot_date TO trade_date")
        except Exception as e:
            logger.warning(f"由于特定原因无法检查列名或自动迁移: {e}")
        
        # 创建成交记录表 (增强版，新增 trade_date)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date DATE NOT NULL,
                trade_time TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                action TEXT NOT NULL,
                price REAL,
                volume INTEGER,
                amount REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 尝试添加新字段（如果表已存在）
        try:
            cursor.execute("ALTER TABLE portfolio_snapshot ADD COLUMN position_pct REAL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE portfolio_snapshot ADD COLUMN cost_price REAL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE portfolio_snapshot ADD COLUMN current_price REAL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE daily_transactions ADD COLUMN trade_date DATE")
        except:
            pass
        
        # 删除旧的唯一索引（不再去重）
        try:
            cursor.execute("DROP INDEX IF EXISTS idx_transaction_unique")
        except:
            pass
        
        conn.commit()
        conn.close()
        
        logger.info(f"数据库初始化完成: {self.db_path}")
    
    def clear_target_data(self, date_str: str, screenshot_type: str):
        """
        清除指定日期和类型的数据
        screenshot_type: "持仓详情" -> 清除 portfolio_snapshot
        screenshot_type: "当日成交" -> 清除 daily_transactions
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
            cursor = conn.cursor()
            
            deleted_count = 0
            
            if screenshot_type == "持仓详情":
                cursor.execute("DELETE FROM portfolio_snapshot WHERE trade_date = ?", (date_str,))
                deleted_count = cursor.rowcount
                logger.info(f"已清除 {date_str} 的旧持仓数据: {deleted_count} 条")
                
            elif screenshot_type == "当日成交":
                cursor.execute("DELETE FROM daily_transactions WHERE trade_date = ?", (date_str,))
                deleted_count = cursor.rowcount
                logger.info(f"已清除 {date_str} 的旧成交数据: {deleted_count} 条")
            
            conn.commit()
            conn.close()
                
        except Exception as e:
            logger.error(f"清除数据失败 ({date_str}, {screenshot_type}): {e}")
            raise

    def insert_portfolio_snapshot(self, records: List[Dict[str, Any]], trade_date: str = None) -> int:
        """
        插入持仓快照数据 (不再自动清除)
        
        Args:
            records: 持仓记录列表
            trade_date: 交易日期，默认为今天
            
        Returns:
            插入的记录数
        """
        if not trade_date:
            trade_date = datetime.now().strftime("%Y-%m-%d")
        
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        except sqlite3.OperationalError as e:
            logger.error(f"数据库连接失败（portfolio_snapshot）: {e}")
            raise
        cursor = conn.cursor()
        
        inserted = 0
        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO portfolio_snapshot 
                    (trade_date, stock_name, market_value, position_pct, 
                     cost_price, current_price, floating_pnl, today_pnl)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade_date,
                    record.get("stock_name", ""),
                    record.get("market_value"),
                    record.get("position_pct", 0),
                    record.get("cost_price"),
                    record.get("current_price"),
                    record.get("floating_pnl"),
                    record.get("today_pnl")
                ))
                inserted += 1
            except Exception as e:
                logger.warning(f"插入持仓记录失败: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"成功插入 {inserted} 条持仓记录 (日期: {trade_date})")
        return inserted
        
    
    def insert_transactions(self, records: List[Dict[str, Any]], trade_date: str = None) -> int:
        """
        插入成交记录 (不再自动清除)
        
        Args:
            records: 成交记录列表
            trade_date: 交易日期，默认为今天
            
        Returns:
            插入的记录数
        """
        if not trade_date:
            trade_date = datetime.now().strftime("%Y-%m-%d")
        
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        except sqlite3.OperationalError as e:
            logger.error(f"数据库连接失败（daily_transactions）: {e}")
            raise
        cursor = conn.cursor()
        
        inserted = 0
        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO daily_transactions 
                    (trade_date, trade_time, stock_name, action, price, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade_date,
                    record.get("trade_time", ""),
                    record.get("stock_name", ""),
                    record.get("action", ""),
                    record.get("price"),
                    record.get("volume"),
                    record.get("amount")
                ))
                inserted += 1
            except Exception as e:
                logger.warning(f"插入成交记录失败: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"成功插入 {inserted} 条成交记录 (日期: {trade_date})")
        return inserted


def get_record_date(record: Dict[str, Any]) -> str:
    """
    获取记录的日期
    优先级:
    1. '上传时间' 字段 (Feishu 系统字段, 毫秒时间戳)
    2. '原始截图' 文件名中的日期 (如 "2026年1月22日...")
    3. Feishu 记录的 'created_time'
    4. 记录中的 '数据日期' 字段
    5. 当前日期
    """
    import re
    data_date = None
    fields = record.get("fields", {})
    
    # 1. 优先检查 '上传时间' 字段 (毫秒时间戳)
    upload_time = fields.get("上传时间")
    if upload_time:
        try:
            dt = datetime.fromtimestamp(upload_time / 1000)
            data_date = dt.strftime("%Y-%m-%d")
            logger.info(f"从 '上传时间' 解析日期: {data_date}")
            return data_date
        except Exception as e:
            logger.warning(f"解析 '上传时间' 失败: {e}")

    # 2. 尝试从文件名提取日期
    # 格式示例: "2026年1月22日 18:33.jpg"
    attachments = fields.get("原始截图", [])
    if isinstance(attachments, list):
        for att in attachments:
            name = att.get("name", "")
            match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", name)
            if match:
                year, month, day = match.groups()
                data_date = f"{year}-{int(month):02d}-{int(day):02d}"
                logger.info(f"从文件名 '{name}' 提取日期: {data_date}")
                return data_date
    
    # 3. 尝试系统字段 created_time (通常不存在或需特定权限)
    created_time = record.get("created_time")
    if created_time:
        try:
            dt = datetime.fromtimestamp(created_time / 1000)
            data_date = dt.strftime("%Y-%m-%d")
            return data_date
        except:
            pass
            
    # 4. 尝试 '数据日期' 字段
    if "数据日期" in fields:
        date_value = fields.get("数据日期")
        if isinstance(date_value, (int, float)):
            data_date = datetime.fromtimestamp(date_value / 1000).strftime("%Y-%m-%d")
        elif isinstance(date_value, str):
            data_date = date_value[:10]
        if data_date:
            return data_date
    
    # 5. 回退到当前日期
    data_date = datetime.now().strftime("%Y-%m-%d")
    logger.warning(f"未能获取日期，回退到当日: {data_date}")
        
    return data_date


def process_single_record(
    client: FeishuClient,
    ai_processor: AIProcessor,
    db_manager: DatabaseManager,
    record: Dict[str, Any]
) -> bool:
    """
    处理单条飞书记录
    
    Args:
        client: 飞书客户端
        ai_processor: AI 处理器
        db_manager: 数据库管理器
        record: 飞书记录
        
    Returns:
        是否处理成功
    """
    record_id = record.get("record_id")
    fields = record.get("fields", {})
    
    # 获取截图类型
    screenshot_type = fields.get("截图类型", "")
    if screenshot_type not in ["持仓详情", "当日成交"]:
        error_msg = f"未知的截图类型: {screenshot_type}"
        logger.error(error_msg)
    # 获取数据日期
    data_date = get_record_date(record)
    logger.info(f"数据日期: {data_date}")
    
    # 获取附件信息
    attachments = fields.get("原始截图", [])
    if not attachments:
        error_msg = "没有找到附件图片"
        logger.error(error_msg)
        client.update_record(record_id, "识别失败", error_msg)
        return False
    
    # 获取第一个附件的 file_token
    attachment = attachments[0] if isinstance(attachments, list) else attachments
    file_token = attachment.get("file_token")
    
    if not file_token:
        error_msg = "附件缺少 file_token"
        logger.error(error_msg)
        client.update_record(record_id, "识别失败", error_msg)
        return False
    
    try:
        # 1. 下载图片
        logger.info(f"正在处理记录 [{record_id}], 类型: {screenshot_type}")
        image_data = client.download_image(file_token)
        
        if not image_data:
            raise Exception("图片下载失败")
        
        # 2. 保存为临时文件
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp_file:
            tmp_file.write(image_data)
            tmp_path = tmp_file.name
            
        try:
            # 3. AI 识别
            records = ai_processor.process_image(tmp_path, screenshot_type)
            
            if not records:
                # 空记录视为成功（可能是空截图），但要记录日志
                client.update_record(record_id, "已完成", "未识别到有效记录")
                return True
                
            # 4. 存入数据库
            if screenshot_type == "持仓详情":
                count = db_manager.insert_portfolio_snapshot(records, trade_date=data_date)
            else:
                count = db_manager.insert_transactions(records, trade_date=data_date)
            
            # 5. 回写状态
            log_text = json.dumps(records, ensure_ascii=False, indent=2)
            client.update_record(record_id, "已完成", f"成功提取 {count} 条记录\n数据日期: {data_date}")
            return True
            
        finally:
            # 清理临时文件
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    except Exception as e:
        import traceback
        error_msg = f"处理出错: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        client.update_record(record_id, "识别失败", error_msg + "\n" + traceback.format_exc())
        return False


def main():
    logger.info("启动 ETL 任务...")
    
    # 不再删除旧数据库，保留历史数据
    # if os.path.exists(DB_PATH): ...
    
    # 检查配置
    if FEISHU_APP_ID == "cli_your_app_id":
        logger.error("请先配置飞书应用凭证！")
        logger.error("修改脚本顶部的 FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_APP_TOKEN, FEISHU_TABLE_ID")
        sys.exit(1)
    
    # 初始化组件
    feishu = FeishuClient(FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_APP_TOKEN, FEISHU_TABLE_ID)
    ai = AIProcessor(OLLAMA_VISION_MODEL)
    db = DatabaseManager(DB_PATH)
    
    logger.info(f"数据库路径: {DB_PATH}")
    logger.info("开始监听飞书表格...")
    
    # 主循环
    while True:
        try:
            # 获取待处理记录
            pending_records = feishu.get_pending_records()
            
            if pending_records:
                logger.info(f"发现 {len(pending_records)} 条待处理记录")
                
                # 1. 识别并清理本次将涉及的数据 (按日期+类型)
                targets_to_clear = set()
                for record in pending_records:
                    d = get_record_date(record)
                    fields = record.get("fields", {})
                    s_type = fields.get("截图类型")
                    if d and s_type:
                        targets_to_clear.add((d, s_type))
                
                for d, s_type in targets_to_clear:
                    logger.info(f"准备更新 {d} 的 {s_type} 数据，正在清除旧记录...")
                    db.clear_target_data(d, s_type)
                
                # 2. 逐条处理
                for record in pending_records:
                    process_single_record(
                        client=feishu,
                        ai_processor=ai,
                        db_manager=db,
                        record=record
                    )
                    
                    # 每条记录处理后短暂休息，避免 API 限流
                    time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("收到中断信号，程序退出")
            break
        except Exception as e:
            logger.error(f"主循环异常: {e}")
        
        # 等待下一次轮询
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
