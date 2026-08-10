# -*- coding: utf-8 -*-
"""
03_adaptive_threshold.py —— 自适应买入阈值 / 仓位
=================================================
根据市场状态调整:
  - 买入门槛(buy_quantile): 仅买入最终分数 >= 该横截面分位的票; 逆境提高门槛,
    只挑最强信号, 过滤掉"勉强入选"的弱信号。
  - 仓位比例(position_scale): 逆境收缩仓位(部分现金), 顺势满仓。这是夏普改善的
    主要来源——在 trend_down / high_vol 降低暴露, 砍掉回撤。

用法:
  from tasks.market_state.config_loader import load_regime_config
  from tasks.market_state.03_adaptive_threshold import AdaptiveThreshold
  at = AdaptiveThreshold(load_regime_config())
  p = at.params('high_vol')            # -> {'buy_quantile':0.85,'position_scale':0.40,'top_k':3}
  keep_idx = at.select(pan, 'final_score', state='high_vol')  # 返回通过门槛的 index
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

ROOT = Path('.').resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class AdaptiveThreshold:
    """状态依赖的买入门槛 + 仓位控制器。"""

    def __init__(self, cfg: dict):
        self.adaptive: Dict[str, dict] = cfg.get('adaptive', {})
        self.fixed: dict = cfg.get('fixed_baseline', {})
        if not self.adaptive:
            raise ValueError('config 缺少 adaptive 段')

    # -- 取参 -------------------------------------------------------------- #
    def params(self, state: str) -> dict:
        """返回某状态的 {buy_quantile, position_scale, top_k}。未知状态回退 default。"""
        if state in self.adaptive:
            return self.adaptive[state]
        # high_vol 优先于趋势判定, 其余未知按 range 处理
        return self.adaptive.get('range', {})

    def fixed_params(self) -> dict:
        """固定权重基线的参数(满仓、无门槛)。"""
        return {
            'buy_quantile': float(self.fixed.get('buy_quantile', 0.0)),
            'position_scale': float(self.fixed.get('position_scale', 1.0)),
            'top_k': int(self.fixed.get('top_k', 5)),
        }

    # -- 选股(应用门槛) ---------------------------------------------------- #
    def select(self, pan: pd.DataFrame, score_col: str = 'final_score',
               state: str = 'range') -> pd.DataFrame:
        """按状态门槛筛选候选:

        1. 取分数 >= buy_quantile 横截面分位的票;
        2. 按分数降序最多取 top_k 只。
        返回筛选后的 DataFrame(保留原 index)。
        """
        p = self.params(state)
        q = float(p.get('buy_quantile', 0.0))
        top_k = int(p.get('top_k', 5))
        if q > 0:
            thr = pan[score_col].quantile(q)
            pan = pan[pan[score_col] >= thr]
        return pan.sort_values(score_col, ascending=False).head(top_k)

    def fixed_select(self, pan: pd.DataFrame,
                     score_col: str = 'final_score') -> pd.DataFrame:
        """固定基线筛选: 无门槛, 取固定 top_k。"""
        p = self.fixed_params()
        top_k = int(p.get('top_k', 5))
        return pan.sort_values(score_col, ascending=False).head(top_k)


# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    sys.path.insert(0, str(ROOT / 'tasks' / 'market_state'))
    from config_loader import load_regime_config
    cfg = load_regime_config()
    at = AdaptiveThreshold(cfg)
    print('各状态参数:')
    for st in ('trend_up', 'range', 'high_vol', 'trend_down'):
        print(f'  {st:10s}: {at.params(st)}')
    print('固定基线:', at.fixed_params())
    # 演示筛选
    np.random.seed(1)
    demo = pd.DataFrame({'final_score': np.random.randn(50)})
    for st in ('trend_up', 'high_vol'):
        sel = at.select(demo, state=st)
        print(f'{st}: 通过门槛 {len(sel)} 只 (buy_q={at.params(st)["buy_quantile"]})')
    print('\n模块自检通过。')
