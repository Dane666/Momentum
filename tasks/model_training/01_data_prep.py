# -*- coding: utf-8 -*-
"""
01_data_prep.py —— LightGBM 训练数据准备
=========================================
把 Phase1 因子面板 + 现有策略信号(breakout/pullback) + 大盘环境 拼成建模宽表:
  - 特征: 21 个因子 + breakout + pullback + regime
  - 标签: 前向收益 fwd5/fwd10/fwd20 (回归) 与 up20 (分类)
  - 时间序列切分: train / val / test (按交易日, 不洗牌)

输出: tasks/model_training/output/model_dataset.parquet
      tasks/model_training/output/split_dates.json  (各集日期边界)
"""
import sys
import json
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path('.').resolve()
for p in [str(ROOT / 'tools'), str(ROOT / 'opt_study'), str(ROOT.parent)]:
    if p not in sys.path:
        sys.path.insert(0, p)
try:
    import momentum
except ImportError:
    s = importlib.util.spec_from_file_location(
        'momentum', ROOT / '__init__.py',
        submodule_search_locations=[str(ROOT)])
    m = importlib.util.module_from_spec(s)
    sys.modules['momentum'] = m
    s.loader.exec_module(m)

import volume_price_scan as VPS
import volume_price_strategy as VS

PANEL = Path('tasks/factor_engineering/output/factors_panel_full.parquet')
OUT = Path('tasks/model_training/output')
OUT.mkdir(parents=True, exist_ok=True)

FACTORS = [
    'hist_vol_20', 'hist_vol_60', 'hist_vol_ratio', 'atr_pct_14',
    'boll_width_20', 'boll_pctb_20', 'turn_chg_5', 'turn_zscore_20',
    'turn_ma_ratio', 'turn_rank_60', 'mfi_14', 'obv', 'obv_ma_ratio',
    'cmf_20', 'main_flow_ratio', 'rsi_14_imp', 'macd_hist_slope',
    'kdj_k', 'kdj_d', 'kdj_j', 'bias_60',
]
# 训练/验证/测试切分边界(按交易日)
SPLIT = {
    'train_end': '2025-06-30',
    'val_end': '2025-12-31',
    # test = 2026-01-01 ~ 最新
}


def main():
    print('[1/4] 读因子面板 ...')
    pan = pd.read_parquet(PANEL)
    pan = pan.sort_values(['code', 'trade_date']).reset_index(drop=True)
    print(f'     面板 shape={pan.shape}')

    print('[2/4] 计算前向收益标签 ...')
    for h in (5, 10, 20):
        pan[f'fwd{h}'] = pan.groupby('code')['close'].transform(
            lambda s: s.shift(-h) / s - 1.0)
    pan['up20'] = (pan['fwd20'] > 0).astype(int)

    print('[3/4] 叠加现有策略信号 + 大盘环境特征 (build_inv, 与扫描一致) ...')
    H = VPS._load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    inv = VS.build_inv(ctx, cal, names, hot_at, regime)

    bo = {(c, ts) for ts, cs in inv['breakout'].items() for c in cs}
    pb = {(c, ts) for ts, cs in inv['pullback'].items() for c in cs}
    print(f'     突破信号事件={len(bo)} | 回踩信号事件={len(pb)}')

    pan['td'] = pan['trade_date'].dt.strftime('%Y-%m-%d')
    bo_df = pd.DataFrame(list(bo), columns=['code', 'td'])
    bo_df['breakout'] = 1
    pb_df = pd.DataFrame(list(pb), columns=['code', 'td'])
    pb_df['pullback'] = 1
    pan = pan.merge(bo_df, on=['code', 'td'], how='left')
    pan = pan.merge(pb_df, on=['code', 'td'], how='left')
    pan[['breakout', 'pullback']] = pan[['breakout', 'pullback']].fillna(0).astype(int)

    reg_df = pd.DataFrame(
        [(ts, r) for ts, r in regime.items()], columns=['td', 'regime_s'])
    reg_map = {'bull': 1, 'ranging': 0, 'bear': -1}
    reg_df['regime'] = reg_df['regime_s'].map(reg_map).fillna(0).astype(int)
    pan = pan.merge(reg_df[['td', 'regime']], on='td', how='left')
    pan['regime'] = pan['regime'].fillna(0).astype(int)

    print('[4/4] 过滤 + 切分 + 保存 ...')
    # 过滤: 去掉仙股/缺标签/缺因子
    pan = pan[pan['close'] >= 1.5]
    feat_cols = FACTORS + ['breakout', 'pullback', 'regime']
    pan = pan.dropna(subset=feat_cols + ['fwd20'])
    # 切分
    t_end = pd.Timestamp(SPLIT['train_end'])
    v_end = pd.Timestamp(SPLIT['val_end'])
    pan['date'] = pan['trade_date']
    pan['split'] = np.where(pan['date'] <= t_end, 'train',
                    np.where(pan['date'] <= v_end, 'val', 'test'))
    print('     各集行数:', dict(pan['split'].value_counts()))
    print('     特征数:', len(feat_cols))
    pan.to_parquet(OUT / 'model_dataset.parquet', index=False)
    (OUT / 'split_dates.json').write_text(json.dumps(SPLIT, indent=2))
    print(f'[ok] -> {OUT/"model_dataset.parquet"}')
    print(f'[ok] -> {OUT/"split_dates.json"}')
    print('特征:', feat_cols)


if __name__ == '__main__':
    main()
