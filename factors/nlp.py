# -*- coding: utf-8 -*-
"""
NLP 因子分析模块
集成 Ollama 模型进行个股新闻情绪分析
"""

import requests
import re
import json
import logging
import warnings
import atexit
import ollama
from bs4 import BeautifulSoup
from .. import config as cfg

logger = logging.getLogger('momentum.factors.nlp')

_OLLAMA_CLIENT = None


def _get_ollama_client():
    """获取可复用的 Ollama 客户端并在退出时关闭"""
    global _OLLAMA_CLIENT
    if _OLLAMA_CLIENT is None:
        try:
            _OLLAMA_CLIENT = ollama.Client()
            if hasattr(_OLLAMA_CLIENT, "close"):
                atexit.register(_OLLAMA_CLIENT.close)
        except Exception as e:
            logger.debug(f"[NLP] Ollama 客户端初始化失败: {e}")
            _OLLAMA_CLIENT = None
    return _OLLAMA_CLIENT


def _close_ollama_client():
    """关闭 Ollama 客户端连接，避免 ResourceWarning"""
    global _OLLAMA_CLIENT
    if _OLLAMA_CLIENT is None:
        return
    try:
        if hasattr(_OLLAMA_CLIENT, "close"):
            _OLLAMA_CLIENT.close()
        elif hasattr(_OLLAMA_CLIENT, "_client") and hasattr(_OLLAMA_CLIENT._client, "close"):
            _OLLAMA_CLIENT._client.close()
    except Exception as e:
        logger.debug(f"[NLP] Ollama 客户端关闭失败: {e}")
    finally:
        _OLLAMA_CLIENT = None

def fetch_stock_news(code: str) -> str:
    """
    获取个股新浪新闻摘要
    Args:
        code: 股票代码 (6位)
    Returns:
        str: 新闻标题拼接字符串
    """
    try:
        symbol = f"sh{code}" if code.startswith('6') else f"sz{code}"
        url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllNewsStock/symbol/{symbol}.phtml"
        
        # 使用 requests 直接获取，设置超时
        resp = requests.get(url, timeout=3, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        resp.encoding = 'gbk'
        
        # 使用 html.parser 避免 lxml 的 DeprecationWarning
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            soup = BeautifulSoup(resp.text, 'html.parser')
        datelist = soup.find('div', class_='datelist')
        
        if not datelist:
            return ""
            
        # 获取前5条新闻标题
        news_list = [a.text.strip() for a in datelist.find_all('a')[:5]]
        return " ".join(news_list)
        
    except Exception as e:
        logger.debug(f"[NLP] {code} 新闻获取失败: {e}")
        return ""

def analyze_sentiment(code: str, dxy_val: float = 100.0, dxy_trend: str = "平稳") -> tuple:
    """
    调用 Ollama 分析个股新闻情绪
    
    Args:
        code: 股票代码
        dxy_val: 美元指数数值
        dxy_trend: 美元指数趋势描述
        
    Returns:
        (score, category): 情绪得分(-1.0~1.0), 逻辑分类
    """
    news = fetch_stock_news(code)
    
    # 如果没有新闻，或者新闻太短，直接返回 0
    if not news or len(news) < 10:
        return 0.0, "无有效新闻"
        
    prompt = f"""
    你是个顶级宏观量化策略师。
    【当前宏观】: 美元指数 {dxy_val} (趋势: {dxy_trend})。
    【分析对象】: {code}
    【最新资讯】: {news}。
    
    【任务】:
    1. 结合“美元流动性”与“个股基本面”进行逻辑推演。
    2. 必须给出 -1.0 (极度利空) 到 1.0 (极度利好) 的情绪评分。
    
    【Few-Shot 参考】:
    - 新闻:"签订巨额海外订单", 美元:"走弱" -> 评分: 0.9 (汇率利好+业绩实锤)
    - 新闻:"公司高管减持", 美元:"走强" -> 评分: -0.8 (流动性收紧+内部看空)
    - 新闻:"无重大消息", 美元:"平稳" -> 评分: 0.1 (随大盘波动)

    【输出要求】:
    严禁输出任何废话，仅返回标准 JSON 格式：
    {{"category": "简短逻辑描述(4字内)", "score": 浮点数值}}
    """
    
    try:
        model = getattr(cfg, 'OLLAMA_MODEL', 'qwen3:8b')
        client = _get_ollama_client()
        try:
            if client is not None:
                response = client.chat(model=model, messages=[{'role': 'user', 'content': prompt}])
            else:
                response = ollama.chat(model=model, messages=[{'role': 'user', 'content': prompt}])
        finally:
            _close_ollama_client()
        res_text = response['message']['content'].strip()
        
        # 提取 JSON
        json_match = re.search(r'\{.*\}', res_text, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            score = max(-1.0, min(1.0, float(data.get('score', 0))))
            category = data.get('category', '常规波动')
            return score, category
        else:
            return 0.0, "解析失败"
            
    except Exception as e:
        logger.debug(f"[NLP] {code} 分析失败: {e}")
        return 0.0, "分析异常"
