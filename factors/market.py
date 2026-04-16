# -*- coding: utf-8 -*-
"""
市场因子模块
- 市场宽度
- 热点板块
"""

import pandas as pd
import logging

logger = logging.getLogger('momentum')


def get_market_breadth_pro(df_real: pd.DataFrame) -> float:
    """
    成交量加权的市场宽度

    Args:
        df_real: 实时行情 DataFrame (需要 涨跌幅, 成交额 字段)

    Returns:
        float: 综合宽度指标 (0-1)
    """
    if df_real is None or df_real.empty:
        return 0.0

    df_real = df_real.copy()
    df_real['amount'] = pd.to_numeric(df_real['成交额'], errors='coerce')
    df_real['涨跌幅'] = pd.to_numeric(df_real['涨跌幅'], errors='coerce')

    total_amount = df_real['amount'].sum()
    if total_amount == 0:
        return 0.0

    rising_amount = df_real[df_real['涨跌幅'] > 0]['amount'].sum()

    # 上涨家数占比
    rising_pct = len(df_real[df_real['涨跌幅'] > 0]) / len(df_real)
    # 上涨成交额占比
    amount_pct = rising_amount / (total_amount + 1e-9)

    # 综合宽度 = (上涨家数占比 * 0.4) + (上涨成交额占比 * 0.6)
    return (rising_pct * 0.4) + (amount_pct * 0.6)


def get_hot_sectors(top_n: int = 5) -> pd.DataFrame:
    """
    获取成交额前N的风口板块

    Args:
        top_n: 返回板块数量

    Returns:
        DataFrame with hot sectors
    """
    from ..data import fetch_realtime_quotes

    try:
        df_sectors = fetch_realtime_quotes(fs='概念板块')
        if df_sectors is None or df_sectors.empty:
            return pd.DataFrame()

        # 字段数值化
        df_sectors['涨跌幅'] = pd.to_numeric(df_sectors['涨跌幅'], errors='coerce').fillna(0)
        df_sectors['成交额'] = pd.to_numeric(df_sectors['成交额'], errors='coerce').fillna(0)

        # 全市场概念板块总成交额
        total_amount = df_sectors['成交额'].sum()

        # 按成交额排序取前N
        df_hot = df_sectors.sort_values('成交额', ascending=False).head(top_n).copy()

        # 计算热度占比
        df_hot['热度占比'] = (df_hot['成交额'] / (total_amount + 1e-9)) * 100

        # 资金属性判定
        def get_fund_status(row):
            change = row['涨跌幅']
            heat = row['热度占比']

            if change > 3.0 and heat > 5.0:
                return "🚀 主力强攻"
            elif change > 0 and heat > 5.0:
                return "📈 温和上行"
            elif change < -1.0 and heat > 5.0:
                return "🚨 巨量下跌"
            elif abs(change) < 1.0 and heat > 8.0:
                return "☁️ 高位分歧"
            else:
                return "☕ 存量博弈"

        df_hot['资金属性'] = df_hot.apply(get_fund_status, axis=1)

        return df_hot

    except Exception as e:
        logger.error(f"[HotSectors] 获取失败: {e}")
        return pd.DataFrame()


def get_market_trend_state(index_code: str = '000300') -> dict:
    """
    基于 ADX/ATR 判断市场趋势状态 (震荡 vs 趋势)

    Args:
        index_code: 指数代码 (默认沪深300)

    Returns:
        dict: {
            'state': 'Ranging' | 'Trending' | 'Transition',
            'state_cn': '震荡市' | '趋势市' | '弱趋势',
            'adx': float,
            'atr': float,
            'close': float
        }
    """
    from ..data import fetch_market_index
    from ..factors.technical import compute_adx, compute_atr

    try:
        df_index = fetch_market_index(index_code)
        if df_index is None or df_index.empty:
            return {}

        # 确保数值类型
        for col in ['high', 'low', 'close', 'open']:
            if col in df_index.columns:
                df_index[col] = pd.to_numeric(df_index[col], errors='coerce')

        adx = compute_adx(df_index)
        atr = compute_atr(df_index)
        
        state_key = "Transition"
        state_cn = "弱趋势/转换期"
        
        if adx < 20:
            state_key = "Ranging"
            state_cn = "震荡市 (Ranging)"
        elif adx > 25:
            state_key = "Trending"
            state_cn = "趋势市 (Trending)"
            
        return {
            'state': state_key,
            'state_cn': state_cn,
            'adx': round(adx, 2),
            'atr': round(atr, 2),
            'close': df_index['close'].iloc[-1]
        }
        
    except Exception as e:
        logger.error(f"[MarketTrend] 判断失败: {e}")
        return {}


def get_dxy_status() -> tuple:
    """
    获取美元指数实时行情并判定趋势
    
    数据源: 默认值 (外部 API 不稳定，使用默认值保证流程通畅)
    
    Returns:
        tuple: (latest_val, trend_desc, impact_score)
        - latest_val: 最新值
        - trend_desc: 趋势描述 (e.g., "强劲", "走弱", "平稳")
        - impact_score: 影响分数 (-1.0 to 1.0, 负数代表利空A股)
    """
    # 美元指数是辅助指标，直接返回默认值，不阻塞主流程
    # 如需实时数据，可手动查看外部网站
    return 107.0, "平稳", 0.0
