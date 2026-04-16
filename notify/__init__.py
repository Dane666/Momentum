# -*- coding: utf-8 -*-
"""
Momentum Notify Module - 通知与 AI 分析
"""
from .feishu import send_feishu_msg, send_feishu_card
from .gemini import get_trading_advice, display_gemini_advice, build_prompt
from .ollama import (
    get_ollama_trading_advice,
    get_quick_advice,
    get_market_time_context,
    build_ollama_prompt,
)

__all__ = [
    # 飞书通知
    'send_feishu_msg', 
    'send_feishu_card',
    # Gemini AI
    'get_trading_advice', 
    'display_gemini_advice', 
    'build_prompt',
    # Ollama 本地模型
    'get_ollama_trading_advice',
    'get_quick_advice',
    'get_market_time_context',
    'build_ollama_prompt',
]

