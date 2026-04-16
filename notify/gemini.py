# -*- coding: utf-8 -*-
"""
Gemini API 集成模块
用于获取 AI 交易建议
"""

import os
import time
import logging
from datetime import datetime
from typing import Optional, List

logger = logging.getLogger('momentum')

# Prompt 模板
TRADING_ADVICE_PROMPT = """你是一名资深 A 股量化交易专家，精通动量策略。

当前时间: {timestamp}
策略版本: Momentum 版本

以下是「{report_type}」的详细数据（包含市场情绪分析和港股通板块分析）:

{report_content}

请基于以上数据，给出实盘操作建议：
1. 首先分析【市场情绪与资金流向】，判断当前市场环境是否适合操作
2. 结合【港股通板块分析】，分析外资偏好和热门板块动向
3. 对于选股结果，分析每只股票的买入时机、仓位建议和止损位
4. 对于持仓诊断，分析每只股票的持有/卖出建议和具体理由
5. 综合考虑市场情绪、资金流向、因子强度等指标，给出当下操作建议

请用中文回答，简洁专业。"""

# 备用模型列表（按优先级排序，2025/2026 可用模型）
FALLBACK_MODELS = [
    "gemini-3.0-flash",
    "gemini-2.5-flash",
    "gemini-2.0-flash",
]


def build_prompt(report_content: str, report_type: str, timestamp: str = None) -> str:
    """
    构建发送给 Gemini 的 Prompt

    Args:
        report_content: 报告内容（选股或持仓诊断）
        report_type: 报告类型 ("尾盘选股" 或 "持仓诊断")
        timestamp: 时间戳，默认为当前时间

    Returns:
        格式化后的 Prompt 字符串
    """
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return TRADING_ADVICE_PROMPT.format(
        timestamp=timestamp,
        report_type=report_type,
        report_content=report_content
    )


def _call_gemini_with_retry(
    client,
    model: str,
    prompt: str,
    max_retries: int = 3,
    initial_delay: float = 5.0
) -> Optional[str]:
    """
    带重试机制的 Gemini API 调用

    Args:
        client: Gemini 客户端
        model: 模型名称
        prompt: 提示词
        max_retries: 最大重试次数
        initial_delay: 初始延迟秒数

    Returns:
        API 返回的文本，失败返回 None
    """
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            logger.info(f"正在调用 Gemini API ({model})... [尝试 {attempt + 1}/{max_retries}]")

            response = client.models.generate_content(
                model=model,
                contents=prompt,
            )

            logger.info("Gemini API 调用成功")
            return response.text

        except Exception as e:
            error_str = str(e)

            # 检查是否是速率限制错误
            if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                # 尝试从错误信息中提取重试时间
                retry_delay = delay
                if "retry in" in error_str.lower():
                    try:
                        # 尝试解析 "retry in Xs" 格式
                        import re
                        match = re.search(r'retry in (\d+\.?\d*)', error_str.lower())
                        if match:
                            retry_delay = float(match.group(1)) + 1  # 额外加1秒
                    except:
                        pass

                if attempt < max_retries - 1:
                    logger.warning(f"API 速率限制，{retry_delay:.1f}秒后重试...")
                    time.sleep(retry_delay)
                    delay *= 2  # 指数退避
                    continue
                else:
                    logger.error(f"API 速率限制，已达最大重试次数")
                    return None

            # 检查是否是模型不存在错误
            elif "404" in error_str or "NOT_FOUND" in error_str:
                logger.error(f"模型 {model} 不存在或不支持")
                return None

            else:
                logger.error(f"Gemini API 调用失败: {e}")
                return None

    return None


def get_trading_advice(
    report_content: str,
    report_type: str,
    api_key: str = None,
    model: str = None,
    timeout: int = None
) -> Optional[str]:
    """
    调用 Gemini API 获取交易建议（带自动重试和备用模型）

    Args:
        report_content: 报告内容
        report_type: 报告类型
        api_key: Gemini API Key (可选，默认从配置读取)
        model: 模型名称 (可选，默认从配置读取)
        timeout: 超时秒数 (可选，默认从配置读取)

    Returns:
        Gemini 返回的交易建议文本，失败返回 None
    """
    # 延迟导入避免循环依赖
    from .. import config as cfg

    # 检查是否启用
    if not getattr(cfg, 'ENABLE_GEMINI_ADVICE', False):
        logger.info("Gemini 建议功能未启用 (ENABLE_GEMINI_ADVICE=False)")
        return None

    # 获取配置
    api_key = api_key or os.environ.get('GEMINI_API_KEY') or getattr(cfg, 'GEMINI_API_KEY', '')
    primary_model = model or getattr(cfg, 'GEMINI_MODEL', 'gemini-2.0-flash')
    timeout = timeout or getattr(cfg, 'GEMINI_TIMEOUT', 30)

    if not api_key:
        logger.warning("Gemini API Key 未配置，请设置环境变量 GEMINI_API_KEY 或在 config.py 中配置")
        return None

    try:
        from google import genai

        # 创建客户端
        client = genai.Client(api_key=api_key)

        # 构建 Prompt
        prompt = build_prompt(report_content, report_type)

        # 构建模型尝试列表（优先使用配置的模型）
        models_to_try = [primary_model]
        for fallback in FALLBACK_MODELS:
            if fallback not in models_to_try:
                models_to_try.append(fallback)

        # 依次尝试各个模型
        for model_name in models_to_try:
            result = _call_gemini_with_retry(client, model_name, prompt)
            if result:
                return result

            # 如果当前模型失败，尝试下一个
            if model_name != models_to_try[-1]:
                logger.info(f"切换到备用模型...")

        logger.error("所有模型均调用失败")
        return None

    except ImportError:
        logger.error("google-genai 库未安装，请执行: pip install google-genai")
        return None
    except Exception as e:
        logger.error(f"Gemini API 调用失败: {e}")
        return None


def display_gemini_advice(advice: str, report_type: str):
    """
    显示 Gemini 建议到控制台

    Args:
        advice: Gemini 返回的建议文本
        report_type: 报告类型
    """
    from ..notify import send_feishu_msg

    print("\n" + "🤖" * 20 + " Gemini AI 建议 " + "🤖" * 20)
    print(f"[{report_type}] AI 实盘操作建议:")
    print("-" * 80)
    print(advice)
    print("🤖" * 50)

    # 发送飞书通知
    send_feishu_msg(f"🤖 Gemini AI 建议 - {report_type}", advice)

