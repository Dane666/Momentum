# -*- coding: utf-8 -*-
"""
情绪因子模块
- 连板高度择时
- 互联互通情绪
- 仓位系数计算
"""

import pandas as pd
import logging

logger = logging.getLogger('momentum')


def get_limit_up_streak() -> tuple:
    """
    获取当日市场最高连板天数，用于情绪周期判断

    Returns:
        (max_streak, emotion_state)
        - emotion_state: 'HOT' (情绪高点,减仓), 'COLD' (冰点,加仓), 'NORMAL'
    """
    from .. import config as cfg
    from ..data import fetch_realtime_quotes

    try:
        df_real = fetch_realtime_quotes(fs='沪深A股')
        if df_real is None or df_real.empty:
            return 0, 'NORMAL'

        df_limit = df_real[
            (pd.to_numeric(df_real['涨跌幅'], errors='coerce') >= 9.8) &
            (~df_real['股票名称'].str.contains('ST'))
        ].copy()

        if df_limit.empty:
            logger.info("[连板] 今日无涨停股")
            return 0, 'COLD'

        # 换手率 < 3% 视为强势连板 (一字板/T字板)
        df_limit['换手率'] = pd.to_numeric(df_limit['换手率'], errors='coerce')
        
        # 检测回退数据 (换手率全为0时，无法准确判断强势涨停)
        is_fallback = df_limit['换手率'].sum() == 0
        if is_fallback:
            # 回退模式: 用涨停总数的 1/3 估算强势涨停
            strong_limit_count = len(df_limit) // 3
            logger.debug(f"[连板] 回退模式，使用涨停数 {len(df_limit)} 的 1/3 估算强势涨停")
        else:
            strong_limit_count = len(df_limit[df_limit['换手率'] < 3.0])

        # 估算最高连板 (经验公式: 强势涨停数 / 5，上限10天)
        estimated_streak = min(10, max(1, strong_limit_count // 5))
        
        logger.debug(f"[连板调试] 涨停数: {len(df_limit)}, 强势涨停(<3%换手): {strong_limit_count}, 估算连板: {estimated_streak}")

        # 情绪状态判定
        if estimated_streak >= cfg.STREAK_EMOTION_HIGH:
            emotion = 'HOT'
            logger.warning(f"[连板] 最高连板约 {estimated_streak} 天，情绪过热，建议减仓")
        elif estimated_streak <= cfg.STREAK_EMOTION_LOW:
            emotion = 'COLD'
            logger.info(f"[连板] 最高连板约 {estimated_streak} 天，情绪冰点，可加仓")
        else:
            emotion = 'NORMAL'
            logger.info(f"[连板] 最高连板约 {estimated_streak} 天，情绪正常")

        return estimated_streak, emotion

    except Exception as e:
        logger.error(f"[连板] 获取失败: {e}")
        return 0, 'NORMAL'


def get_connect_sentiment() -> tuple:
    """
    获取"互联互通"标的情绪 (替代已停止更新的北向资金流向)
    逻辑: 计算沪深股通标的成交额的涨跌失衡度

    Returns:
        (score, trend)
        - score: 情绪得分 (-1.0 ~ 1.0) = (涨股成交 - 跌股成交) / 总成交
        - trend: 'INFLOW' (score > 0.05), 'OUTFLOW' (score < -0.05), 'NEUTRAL'
    """
    from ..data import fetch_realtime_quotes

    try:
        # 获取全口径互联互通标的 (沪股通+深股通)
        df_sh = fetch_realtime_quotes(fs='沪股通')
        df_sz = fetch_realtime_quotes(fs='深股通')

        frames = []
        if df_sh is not None and not df_sh.empty:
            frames.append(df_sh)
        if df_sz is not None and not df_sz.empty:
            frames.append(df_sz)

        if not frames:
            logger.info("[Connect] 无数据可用")
            return 0.0, 'NEUTRAL'

        df = pd.concat(frames, ignore_index=True)

        # 数据清洗
        df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)

        total_amount = df['成交额'].sum()
        if total_amount == 0:
            return 0.0, 'NEUTRAL'

        # 计算多空力量对比 (Power Imbalance)
        up_amount = df[df['涨跌幅'] > 0]['成交额'].sum()
        down_amount = df[df['涨跌幅'] < 0]['成交额'].sum()

        # 情绪得分: 净主动买入占比 (代理指标)
        score = (up_amount - down_amount) / total_amount

        # 判定趋势 (阈值: 5% 失衡度)
        if score > 0.05:
            trend = 'INFLOW'
            logger.info(f"[外资情绪] 积极看多 (强度: {score:.1%})")
        elif score < -0.05:
            trend = 'OUTFLOW'
            logger.warning(f"[外资情绪] 谨慎撤离 (强度: {score:.1%})")
        else:
            trend = 'NEUTRAL'
            logger.info(f"[外资情绪] 多空平衡 (强度: {score:.1%})")

        return score, trend

    except Exception as e:
        logger.error(f"[Connect] 情绪计算失败: {e}")
        return 0.0, 'NEUTRAL'


def get_position_multiplier(emotion_state: str, connect_trend: str) -> float:
    """
    根据市场情绪和外资动向计算仓位系数

    Args:
        emotion_state: 'HOT', 'COLD', 'NORMAL'
        connect_trend: 'INFLOW', 'OUTFLOW', 'NEUTRAL'

    Returns:
        float: 仓位系数 (0.5 - 1.5)
    """
    base = 1.0

    # 情绪调整
    if emotion_state == 'HOT':
        base *= 0.7  # 情绪高点减仓
    elif emotion_state == 'COLD':
        base *= 1.3  # 情绪冰点加仓

    # 外资调整
    if connect_trend == 'INFLOW':
        base *= 1.1
    elif connect_trend == 'OUTFLOW':
        base *= 0.9

    return max(0.5, min(1.5, base))
