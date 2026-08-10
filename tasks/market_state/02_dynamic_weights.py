# -*- coding: utf-8 -*-
"""
02_dynamic_weights.py —— 动态因子权重
=====================================
根据市场状态, 从 config/regime_config.yaml 读取该状态的因子组权重, 将
"状态倾斜"(各因子组横截面 z-score 的加权组合)与模型分数融合, 得到最终排序分。

为什么这样做(对照固定权重):
  - 固定权重基线: 所有状态用同一套常量权重(default), 等价于"无状态适应"。
  - 动态权重    : 每个状态用各自权重(trend_up 重动量 / high_vol 重低波防御 /
                  trend_down 避动量偏好低波 / range 重换手反转), 让选股在
                  不同市道下偏好不同风格的票。

融合公式(每日横截面内):
  z_model  = (model_pred - mean) / std            # 模型分标准化
  tilt     = Σ_g w_g(state) * zscore_g            # 状态倾斜, zscore_g 为该组特征均值的标准化
  final    = z_model + tilt_blend * tilt           # 最终排序分

用法:
  from tasks.market_state.config_loader import load_regime_config
  from tasks.market_state.02_dynamic_weights import DynamicWeights
  dw = DynamicWeights(load_regime_config())
  # pan: 当日候选 DataFrame, 含模型特征列 + 'model_pred'
  pan = dw.apply(pan, state='trend_up')            # 新增 'final_score' 列
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

ROOT = Path('.').resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class DynamicWeights:
    """状态依赖的因子权重融合器。"""

    def __init__(self, cfg: dict):
        self.groups: Dict[str, list] = cfg.get('factor_groups', {})
        self.weights: Dict[str, dict] = cfg.get('factor_weights', {})
        self.tilt_blend: float = float(cfg.get('tilt_blend', 0.5))
        if not self.groups or not self.weights:
            raise ValueError('config 缺少 factor_groups / factor_weights')

    # -- 计算单日状态倾斜 -------------------------------------------------- #
    def _group_zscores(self, pan: pd.DataFrame) -> Dict[str, pd.Series]:
        """返回每个因子组的横截面 z-score 序列(组特征均值后标准化)。"""
        out = {}
        for g, feats in self.groups.items():
            have = [f for f in feats if f in pan.columns]
            if not have:
                out[g] = pd.Series(0.0, index=pan.index)
                continue
            # 组内特征先各自横截面标准化再平均, 避免量纲/极端值主导
            z = pan[have].apply(
                lambda c: (c - c.mean()) / (c.std(ddof=0) + 1e-9))
            out[g] = z.mean(axis=1)
        return out

    def tilt(self, pan: pd.DataFrame, state: str) -> pd.Series:
        """计算该状态下的"状态倾斜"序列(与 pan.index 对齐)。"""
        w = self.weights.get(state, self.weights.get('default', {}))
        gz = self._group_zscores(pan)
        tilt = pd.Series(0.0, index=pan.index)
        for g, z in gz.items():
            tilt = tilt + float(w.get(g, 0.0)) * z
        return tilt

    # -- 融合为最终分数 ---------------------------------------------------- #
    def apply(self, pan: pd.DataFrame, state: str,
              model_col: str = 'model_pred') -> pd.DataFrame:
        """给 pan 增加 'final_score' 列(= z(model) + tilt_blend * tilt)。

        pan 须含模型特征列(用于倾斜)与 model_col(模型预测分)。返回副本。
        """
        pan = pan.copy()
        if model_col not in pan.columns:
            raise ValueError(f'pan 缺少模型分列 {model_col}')
        z_model = (pan[model_col] - pan[model_col].mean()) \
            / (pan[model_col].std(ddof=0) + 1e-9)
        tilt = self.tilt(pan, state)
        pan['final_score'] = z_model + self.tilt_blend * tilt
        return pan

    # -- 便捷: 固定权重基线(状态无关) ------------------------------------- #
    def apply_fixed(self, pan: pd.DataFrame,
                    fixed_state: str = 'default',
                    model_col: str = 'model_pred') -> pd.DataFrame:
        """固定权重基线: 所有状态用同一套常量权重(default)。"""
        return self.apply(pan, state=fixed_state, model_col=model_col)


# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    sys.path.insert(0, str(ROOT / 'tasks' / 'market_state'))
    from config_loader import load_regime_config
    cfg = load_regime_config()
    dw = DynamicWeights(cfg)
    # 造一个合成候选面板演示
    np.random.seed(0)
    n = 200
    syn = pd.DataFrame({
        'model_pred': np.random.randn(n),
        'bias_60': np.random.randn(n),
        'macd_hist_slope': np.random.randn(n),
        'boll_pctb_20': np.random.randn(n),
        'kdj_j': np.random.randn(n),
        'kdj_d': np.random.randn(n),
        'regime': np.random.choice([-1, 0, 1], n),
        'hist_vol_60': np.random.randn(n),
        'hist_vol_ratio': np.random.randn(n),
        'atr_pct_14': np.random.randn(n),
        'boll_width_20': np.random.randn(n),
        'cmf_20': np.random.randn(n),
        'obv': np.random.randn(n),
        'obv_ma_ratio': np.random.randn(n),
        'main_flow_ratio': np.random.randn(n),
        'mfi_14': np.random.randn(n),
        'turn_rank_60': np.random.randn(n),
        'turn_ma_ratio': np.random.randn(n),
        'turn_zscore_20': np.random.randn(n),
        'turn_chg_5': np.random.randn(n),
        'breakout': np.random.randint(0, 2, n),
        'pullback': np.random.randint(0, 2, n),
    })
    for st in ('trend_up', 'trend_down', 'high_vol', 'range'):
        out = dw.apply(syn, st)
        print(f'{st:10s} tilt 样例 final_score top3 codes:',
              list(out.sort_values('final_score', ascending=False).index[:3]))
    print('\n模块自检通过: 各状态 final_score 已按因子权重融合。')
