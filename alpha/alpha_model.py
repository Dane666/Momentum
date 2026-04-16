# -*- coding: utf-8 -*-
"""
Alpha Model - Alpha因子合成与行业中性化

统一实盘选股和回测的Alpha计算逻辑，确保两者行为完全一致。

Alpha 合成公式:
    Alpha_T = 0.30 × Mom5 + 0.10 × Mom20 + 0.25 × Sharpe 
              - 0.15 × ChipRate + 0.20 × BigOrder + NLP × nlp_w

其中权重会根据市场成交量动态调整 NLP 权重。
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Optional, Dict, List
import logging

logger = logging.getLogger('momentum.alpha')


# ==================== 因子列定义 ====================
# 今日因子列 (T日截面)
FACTOR_COLS_TODAY = [
    'mom_5_t',      # 5日动量
    'mom_20_t',     # 20日动量
    'sharpe_t',     # 夏普比率
    'vr_t',         # 量比
    'turnover_t',   # 换手率
    'chip_rate',    # 筹码集中度
    'big_order_t',  # 大单因子
]

# 昨日因子列 (T-1日截面，用于计算趋势)
FACTOR_COLS_YESTERDAY = [
    'mom_5_y',
    'mom_20_y',
    'sharpe_y',
    'vr_y',
    'big_order_y',
]

# Alpha 合成权重 (与原代码完全一致)
ALPHA_WEIGHTS = {
    'mom_5': 0.30,
    'mom_20': 0.10,
    'sharpe': 0.25,
    'chip_rate': -0.15,  # 负权重：筹码分散为正信号
    'big_order': 0.20,
}


class AlphaModel:
    """
    Alpha 因子模型
    
    负责:
    1. 风格中性化 (按 style_group 分组标准化)
    2. Alpha 得分合成
    3. Alpha 趋势计算
    4. 量比异常惩罚
    
    Attributes:
        market_total_amount: 市场总成交额 (用于动态调整NLP权重)
        vol_surge_limit: 量比惩罚阈值
    """
    
    def __init__(
        self,
        market_total_amount: float = 0.0,
        vol_surge_limit: float = 3.0
    ):
        """
        初始化 Alpha 模型
        
        Args:
            market_total_amount: 市场总成交额 (元)
            vol_surge_limit: 量比超过此值时应用 Sigmoid 惩罚
        """
        self.market_total_amount = market_total_amount
        self.vol_surge_limit = vol_surge_limit
    
    def neutralize_and_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        风格中性化并计算 Alpha 得分
        
        统一入口，替代原有的:
        - engine.industry_neutralization_with_trend()
        - simulator._industry_neutralization()
        
        Args:
            df: 包含因子列的 DataFrame
            
        Returns:
            添加了 alpha_score, alpha_y, alpha_trend 列的 DataFrame
        """
        if df.empty:
            return df
        
        # 1. 确保所有因子列存在
        df = self._ensure_factor_columns(df)
        
        # 2. 风格中性化 (Z-Score 标准化)
        df = self._apply_z_score_normalization(df)
        
        # 3. 计算 Alpha 得分
        df = self._compute_alpha_scores(df)
        
        # 4. 应用量比异常惩罚
        df = self._apply_vol_surge_penalty(df)
        
        return df
    
    def _ensure_factor_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """确保所有因子列存在，缺失列填充为 0"""
        df = df.copy()
        
        for col in FACTOR_COLS_TODAY + FACTOR_COLS_YESTERDAY:
            if col not in df.columns:
                df[col] = 0.0
        
        # 确保 style_group 列存在
        if 'style_group' not in df.columns:
            df['style_group'] = 'SmallCap'
        
        # 确保 NLP 和 HK 加分列存在
        if 'nlp_score' not in df.columns:
            df['nlp_score'] = 0.0
        if 'hk_bonus' not in df.columns:
            df['hk_bonus'] = 0.0
            
        return df
    
    def _apply_z_score_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按 style_group 分组进行 Z-Score 标准化
        
        公式: z = (x - mean) / (std + 1e-9)
        """
        import warnings
        
        # 保存分组列
        style_group_series = df['style_group'].copy()
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            
            # 今日因子标准化
            for col in FACTOR_COLS_TODAY:
                z_col = f'z_{col}'
                df[z_col] = df.groupby('style_group')[col].transform(
                    lambda x: (x - x.mean()) / (x.std() + 1e-9)
                )
            
            # 昨日因子标准化
            for col in FACTOR_COLS_YESTERDAY:
                z_col = f'z_{col}'
                df[z_col] = df.groupby('style_group')[col].transform(
                    lambda x: (x - x.mean()) / (x.std() + 1e-9)
                )
        
        # 恢复分组列
        df['style_group'] = style_group_series.values
        
        return df
    
    def _compute_alpha_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 Alpha 得分
        
        Alpha_T = Σ(权重 × Z-Score因子) + NLP得分 + HK加分
        Alpha_Y = 昨日截面 Alpha
        Alpha_Trend = Alpha_T - Alpha_Y
        """
        from .. import config as cfg
        
        # 根据市场成交量动态调整 NLP 权重
        nlp_weight = self._get_dynamic_nlp_weight()
        remaining_weight = 1.0 - nlp_weight
        
        # 安全获取 Z-Score 列
        def safe_get(col: str) -> pd.Series:
            return df[col] if col in df.columns else pd.Series(0.0, index=df.index)
        
        # Alpha_T (今日 Alpha)
        df['alpha_score'] = (
            safe_get('z_mom_5_t') * remaining_weight * ALPHA_WEIGHTS['mom_5'] +
            safe_get('z_mom_20_t') * remaining_weight * ALPHA_WEIGHTS['mom_20'] +
            safe_get('z_sharpe_t') * remaining_weight * ALPHA_WEIGHTS['sharpe'] +
            safe_get('z_chip_rate') * remaining_weight * ALPHA_WEIGHTS['chip_rate'] +
            safe_get('z_big_order_t') * remaining_weight * ALPHA_WEIGHTS['big_order'] +
            df['nlp_score'] * nlp_weight +
            df['hk_bonus']
        )
        
        # Alpha_Y (昨日 Alpha，用于计算趋势)
        df['alpha_y'] = (
            safe_get('z_mom_5_y') * ALPHA_WEIGHTS['mom_5'] +
            safe_get('z_mom_20_y') * ALPHA_WEIGHTS['mom_20'] +
            safe_get('z_sharpe_y') * ALPHA_WEIGHTS['sharpe'] +
            safe_get('z_chip_rate') * ALPHA_WEIGHTS['chip_rate'] +
            safe_get('z_big_order_y') * ALPHA_WEIGHTS['big_order']
        )
        
        # Alpha 趋势 (正向趋势表示动能增强)
        df['alpha_trend'] = df['alpha_score'] - df['alpha_y']
        
        return df
    
    def _get_dynamic_nlp_weight(self) -> float:
        """
        根据市场成交量动态计算 NLP 权重
        
        - 缩量市 (< 8000亿): NLP权重 0.5 (更依赖情绪)
        - 爆量市 (> 2万亿): NLP权重 0.2 (更依赖量化)
        - 常规市: NLP权重 0.3
        """
        from .. import config as cfg
        
        if self.market_total_amount < cfg.MARKET_AMOUNT_LOW:
            return cfg.NLP_WEIGHT_LOW_VOL   # 0.5
        elif self.market_total_amount > cfg.MARKET_AMOUNT_HIGH:
            return cfg.NLP_WEIGHT_HIGH_VOL  # 0.2
        else:
            return cfg.NLP_WEIGHT_NORMAL    # 0.3
    
    def _apply_vol_surge_penalty(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        量比异常惩罚
        
        当量比超过阈值时，使用 Sigmoid 函数降低 Alpha 得分，
        避免追涨量能异常放大的股票。
        
        公式: Alpha *= 1 / (1 + exp(2 * (vr - 4.5)))
        """
        def sigmoid_penalty(row):
            vr = row.get('vr_t', 0)
            if vr > self.vol_surge_limit:
                penalty = 1 / (1 + np.exp(2 * (vr - 4.5)))
                return row['alpha_score'] * penalty
            return row['alpha_score']
        
        df['alpha_score'] = df.apply(sigmoid_penalty, axis=1)
        
        return df


# ==================== 便捷函数 ====================

def industry_neutralization(
    df: pd.DataFrame,
    market_total_amount: float = 0.0,
    vol_surge_limit: float = 3.0
) -> pd.DataFrame:
    """
    行业中性化并计算 Alpha 得分 (便捷函数)
    
    Args:
        df: 包含因子列的 DataFrame
        market_total_amount: 市场总成交额
        vol_surge_limit: 量比惩罚阈值
        
    Returns:
        添加了 alpha_score, alpha_y, alpha_trend 列的 DataFrame
    """
    model = AlphaModel(
        market_total_amount=market_total_amount,
        vol_surge_limit=vol_surge_limit
    )
    return model.neutralize_and_score(df)


def compute_alpha_score(
    df: pd.DataFrame,
    nlp_weight: float = 0.3
) -> pd.DataFrame:
    """
    计算 Alpha 得分 (简化版，用于回测)
    
    Args:
        df: 已标准化的 DataFrame
        nlp_weight: NLP 权重
        
    Returns:
        添加了 alpha_score 列的 DataFrame
    """
    remaining_weight = 1.0 - nlp_weight
    
    df['alpha_score'] = (
        df.get('z_mom_5_t', 0) * remaining_weight * 0.30 +
        df.get('z_mom_20_t', 0) * remaining_weight * 0.10 +
        df.get('z_sharpe_t', 0) * remaining_weight * 0.25 +
        df.get('z_chip_rate', 0) * remaining_weight * -0.15 +
        df.get('z_big_order_t', 0) * remaining_weight * 0.20 +
        df.get('nlp_score', 0) * nlp_weight +
        df.get('hk_bonus', 0)
    )
    
    return df
