# -*- coding: utf-8 -*-
"""
02_feature_selection.py —— 特征筛选
====================================
- 在 train 集上训练基线 LightGBM(回归 fwd20), 用 val 评估
- 输出特征重要性排序 + 因子相关性矩阵
- 按「保留高重要性、剔除高相关冗余对」给出推荐子集(selected_features.json), 目标 Top 20-30
"""
import sys
import json
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb

ROOT = Path('.')
OUT = Path('tasks/model_training/output')
DS = OUT / 'model_dataset.parquet'

FEATURES = [
    'hist_vol_20', 'hist_vol_60', 'hist_vol_ratio', 'atr_pct_14',
    'boll_width_20', 'boll_pctb_20', 'turn_chg_5', 'turn_zscore_20',
    'turn_ma_ratio', 'turn_rank_60', 'mfi_14', 'obv', 'obv_ma_ratio',
    'cmf_20', 'main_flow_ratio', 'rsi_14_imp', 'macd_hist_slope',
    'kdj_k', 'kdj_d', 'kdj_j', 'bias_60', 'breakout', 'pullback', 'regime',
]
TARGET = 'fwd20'
CORR_THRESH = 0.85  # 高于此相关的冗余对, 保留重要性更高者


def main():
    df = pd.read_parquet(DS)
    tr = df[df.split == 'train']
    va = df[df.split == 'val']
    Xtr, ytr = tr[FEATURES], tr[TARGET]
    Xva, yva = va[FEATURES], va[TARGET]

    print('[1/3] 训练基线 LightGBM ...')
    base = lgb.LGBMRegressor(
        n_estimators=400, learning_rate=0.03, num_leaves=31,
        min_child_samples=200, subsample=0.8, colsample_bytree=0.8,
        reg_lambda=1.0, random_state=42, n_jobs=-1, verbosity=-1)
    base.fit(Xtr, ytr, eval_X=Xva, eval_y=yva,
             eval_metric='rmse', callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)])

    # 评估
    def rmse(a, b):
        return float(np.sqrt(np.mean((a - b) ** 2)))

    tr_pred = base.predict(Xtr)
    va_pred = base.predict(Xva)
    print(f'     train RMSE={rmse(ytr, tr_pred):.5f}  val RMSE={rmse(yva, va_pred):.5f}')

    imp = pd.Series(base.feature_importances_, index=FEATURES).sort_values(ascending=False)
    imp_df = imp.reset_index()
    imp_df.columns = ['feature', 'importance']
    imp_df['rel'] = (imp_df['importance'] / imp_df['importance'].sum()).round(4)
    imp_df.to_csv(OUT / 'feature_importance.csv', index=False)
    print('\n[2/3] Top 特征重要性:')
    print(imp_df.head(12).to_string(index=False))

    print('\n[3/3] 相关性冗余分析 + 生成推荐子集 ...')
    corr = df[FEATURES].corr().abs()
    corr.to_csv(OUT / 'model_feature_corr.csv')
    # 按重要性从高到低, 跳过与已选特征相关>阈值的
    selected = []
    for f in imp.index:
        if f in selected:
            continue
        redundant = False
        for s in selected:
            if corr.loc[f, s] > CORR_THRESH:
                redundant = True
                break
        if not redundant:
            selected.append(f)
    sel_df = pd.DataFrame({'feature': selected})
    sel_df['importance'] = sel_df['feature'].map(imp.to_dict())
    sel_df.to_csv(OUT / 'selected_features.csv', index=False)
    (OUT / 'selected_features.json').write_text(
        json.dumps({'features': selected, 'n': len(selected)}, indent=2))

    # 冗余对报告
    pairs = []
    for i in range(len(FEATURES)):
        for j in range(i + 1, len(FEATURES)):
            c = corr.iloc[i, j]
            if c > CORR_THRESH:
                pairs.append((FEATURES[i], FEATURES[j], round(float(c), 3)))
    pairs.sort(key=lambda x: -x[2])
    lines = ['# 特征筛选报告\n', f'- 基线 val RMSE={rmse(yva, va_pred):.5f}',
             f'- 推荐特征数={len(selected)} (原始 {len(FEATURES)})',
             '- 高相关冗余对(>%.2f, 已保留重要性更高者):\n' % CORR_THRESH]
    for a, b, c in pairs:
        lines.append(f'  - {a} ~ {b}: {c}')
    (OUT / 'feature_selection_report.md').write_text('\n'.join(lines), encoding='utf-8')

    print(f'\n     推荐特征({len(selected)}): {selected}')
    print(f'[ok] -> selected_features.json / feature_importance.csv / model_feature_corr.csv')


if __name__ == '__main__':
    main()
