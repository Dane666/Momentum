# -*- coding: utf-8 -*-
"""
03_select_topk.py —— 输出 Top-K 推荐
====================================
从 02 的 scores 取 Top-K(默认10), 排除已持仓/已计划(读 data/picks_tracking.json 去重),
输出 tasks/model_inference/output/topk_YYYYMMDD.json + .csv(供 Bark 推送 / position_monitor 登记)。
"""
import sys
import json
import glob
import importlib.util
from pathlib import Path

import pandas as pd

SCRIPT = Path(__file__).resolve().parent
OUT = Path('tasks/model_inference/output')
K = 10

# 路径注入(与 02 一致): 保证 tools/ 下模块在本地与 CI 均可导入
ROOT = Path('.').resolve()
for _p in [str(ROOT / 'tools'), str(ROOT / 'opt_study'), str(ROOT.parent)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
import volume_price_scan as VPS


def latest_scores():
    files = sorted(glob.glob(str(OUT / 'scores_20*.csv')))
    return files[-1] if files else None


def load_held_codes():
    """读统一真相源, 排除已 HOLDING/MANUAL/PLAN 的票, 避免重复推荐。"""
    p = Path('data/picks_tracking.json')
    if not p.exists():
        return set()
    try:
        data = json.loads(p.read_text())
    except Exception:
        return set()
    recs = data if isinstance(data, list) else data.get('picks', [])
    held = set()
    for rec in recs:
        st = rec.get('status')
        if st in ('HOLDING', 'MANUAL', 'PLAN', 'TRIGGERED'):
            code = rec.get('code')
            if code is not None:
                held.add(str(code).zfill(6))   # 与 scores 的零填充字符串对齐
    return held


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--k', type=int, default=K)
    ap.add_argument('--scores', default=None)
    a = ap.parse_args()

    sf = a.scores or latest_scores()
    if sf is None:
        raise SystemExit('未找到 scores 文件, 请先运行 02_generate_scores.py')
    scores = pd.read_csv(sf)
    # 代码统一为零填充字符串(CSV 中 int 会丢前导零, 须与 held 集合对齐)
    scores['code'] = scores['code'].astype(str).str.zfill(6)
    held = load_held_codes()
    if held:
        scores = scores[~scores['code'].isin(held)]
        print(f'[select] 已排除 {len(held)} 只在持仓/计划中的票')
    top = scores.head(a.k)
    date = pd.to_datetime(top['trade_date']).max().strftime('%Y%m%d') if len(top) else 'NA'

    picks = []
    names = VPS._load_names()
    for _, r in top.iterrows():
        code = r['code']
        picks.append({
            'code': code,
            'name': names.get(code, code),
            'strategy': 'LGBM_V1',
            'score': round(float(r['pred']), 6),
            'close': round(float(r['close']), 2),
            'date': str(r['trade_date'])[:10],
        })
    # 最后兜底: 名字含 ST/*ST 的票绝不推送(02 候选池已过滤, 此处双保险)
    before = len(picks)
    picks = [p for p in picks if 'ST' not in str(p['name']).upper()]
    if len(picks) < before:
        print(f'[select] 兜底剔除 {before - len(picks)} 只 ST/*ST(02 应已过滤, 检查上游)')

    out_json = OUT / f'topk_{date}.json'
    out_csv = OUT / f'topk_{date}.csv'
    out_json.write_text(json.dumps(
        {'date': date, 'k': a.k, 'model': 'model_v1', 'picks': picks},
        indent=2, ensure_ascii=False))
    top[['code', 'pred', 'close']].to_csv(out_csv, index=False)

    print(f'[select] Top{a.k} 推荐 ({len(top)} 只):')
    for p in picks:
        print(f'   {p["code"]}  score={p["score"]:.4f}  close={p["close"]}')
    print(f'[ok] -> {out_json}')
    print(f'[ok] -> {out_csv}')
    if not picks:
        print('[warn] 0 候选(可能被持仓全排除或当日无数据)')


if __name__ == '__main__':
    main()
