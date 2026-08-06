# -*- coding: utf-8 -*-
"""
04_model_validation.py —— 模型回测验证 + 过拟合检测
====================================================
- 在 test 集(2026-01-01+)逐日对所有可投股票打分, 选 Top-K(K=10)
- 统计模型组合的前向 5/10/20 日收益与胜率
- 对比规则基线: 每日 breakout∪pullback 信号股(等权)的前向收益与胜率 + 信号覆盖天数
- 报告 train/val/test RMSE 与过拟合比
"""
import sys
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path('.')
OUT = Path('tasks/model_training/output')
MODELS = Path('tasks/model_training/models')
K = 10


def main():
    print('[1/4] 加载模型 + 测试集 ...')
    with open(MODELS / 'model_v1.pkl', 'rb') as f:
        bundle = pickle.load(f)
    model, feats, target = bundle['model'], bundle['features'], bundle['target']
    df = pd.read_parquet(OUT / 'model_dataset.parquet')
    te = df[df.split == 'test'].copy()
    print(f'     test 行数={len(te)} 特征={len(feats)}')

    print('[2/4] 逐日打分 + Top-K 选择 ...')
    te = te.dropna(subset=feats)
    te['pred'] = model.predict(te[feats])
    te['date'] = te['trade_date']

    recs = []
    rule_recs = []
    test_dates = sorted(te['date'].unique())
    for d in test_dates:
        day = te[te['date'] == d]
        if day.empty:
            continue
        topk = day.sort_values('pred', ascending=False).head(K)
        for _, r in topk.iterrows():
            recs.append((d, r['code'], r['fwd5'], r['fwd10'], r['fwd20']))
        # 规则基线: 当日信号股
        sig = day[(day['breakout'] == 1) | (day['pullback'] == 1)]
        for _, r in sig.iterrows():
            rule_recs.append((d, r['code'], r['fwd5'], r['fwd10'], r['fwd20']))

    mdf = pd.DataFrame(recs, columns=['date', 'code', 'fwd5', 'fwd10', 'fwd20'])
    rdf = pd.DataFrame(rule_recs, columns=['date', 'code', 'fwd5', 'fwd10', 'fwd20'])

    def summarize(d):
        n = len(d)
        if n == 0:
            return None
        return {
            'n_events': n,
            'n_days': d['date'].nunique(),
            'fwd5': float(d['fwd5'].mean()), 'fwd5_win': float((d['fwd5'] > 0).mean()),
            'fwd10': float(d['fwd10'].mean()), 'fwd10_win': float((d['fwd10'] > 0).mean()),
            'fwd20': float(d['fwd20'].mean()), 'fwd20_win': float((d['fwd20'] > 0).mean()),
        }

    msum = summarize(mdf)
    rsum = summarize(rdf)
    print('\n[3/4] 结果对比 (test 集, 2026-01-01+):')
    print(f'{"指标":14s} {"模型Top{K}":>16s} {"规则信号基线":>16s}')
    if msum and rsum:
        print(f'{"样本事件":14s} {msum["n_events"]:>16d} {rsum["n_events"]:>16d}')
        print(f'{"覆盖交易日":14s} {msum["n_days"]:>16d} {rsum["n_days"]:>16d}')
        print(f'{"20日收益":14s} {msum["fwd20"]*100:>15.2f}% {rsum["fwd20"]*100:>15.2f}%')
        print(f'{"20日胜率":14s} {msum["fwd20_win"]*100:>14.1f}% {rsum["fwd20_win"]*100:>14.1f}%')
        print(f'{"10日收益":14s} {msum["fwd10"]*100:>15.2f}% {rsum["fwd10"]*100:>15.2f}%')
        print(f'{"10日胜率":14s} {msum["fwd10_win"]*100:>14.1f}% {rsum["fwd10_win"]*100:>14.1f}%')
        print(f'{"5日收益":14s} {msum["fwd5"]*100:>15.2f}% {rsum["fwd5"]*100:>15.2f}%')
        print(f'{"5日胜率":14s} {msum["fwd5_win"]*100:>14.1f}% {rsum["fwd5_win"]*100:>14.1f}%')

    print('\n[4/4] 过拟合检测 ...')
    bp = json.loads((OUT / 'best_params.json').read_text())
    print(f'     train RMSE={bp["train_rmse"]:.5f}  val RMSE={bp["val_rmse"]:.5f}'
          f'  过拟合比(val/train)={bp["overfit_ratio"]:.3f}')
    verdict = 'OK (比值<1.3)' if bp['overfit_ratio'] < 1.3 else '⚠ 可能过拟合 (比值>=1.3)'
    print(f'     结论: {verdict}')

    # 保存
    out = {
        'model_topk': msum, 'rule_baseline': rsum,
        'train_rmse': bp['train_rmse'], 'val_rmse': bp['val_rmse'],
        'overfit_ratio': bp['overfit_ratio'], 'verdict': verdict, 'K': K,
    }
    (OUT / 'model_validation.json').write_text(json.dumps(out, indent=2, ensure_ascii=False))
    mdf.to_csv(OUT / 'model_topk_test_picks.csv', index=False)
    lines = ['# LightGBM 模型验证报告 (test 集)\n',
             f'- 模型 Top{K} 每日选股 | 规则基线=每日 breakout∪pullback 信号股(等权)\n']
    if msum and rsum:
        lines.append(f'- 模型: 20日收益 {msum["fwd20"]*100:.2f}% / 胜率 {msum["fwd20_win"]*100:.1f}%'
                     f' / 覆盖 {msum["n_days"]} 交易日')
        lines.append(f'- 规则: 20日收益 {rsum["fwd20"]*100:.2f}% / 胜率 {rsum["fwd20_win"]*100:.1f}%'
                     f' / 覆盖 {rsum["n_days"]} 交易日')
        d_ret = (msum['fwd20'] - rsum['fwd20']) * 100
        d_win = (msum['fwd20_win'] - rsum['fwd20_win']) * 100
        lines.append(f'- 相对规则基线: 20日收益 Δ{d_ret:+.2f}pp / 胜率 Δ{d_win:+.1f}pp')
    lines.append(f'- 过拟合比(val/train)={bp["overfit_ratio"]:.3f} -> {verdict}')
    (OUT / 'model_validation_report.md').write_text('\n'.join(lines), encoding='utf-8')
    print(f'[ok] -> {OUT/"model_validation.json"} / model_validation_report.md')


if __name__ == '__main__':
    main()
