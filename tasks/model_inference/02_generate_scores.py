# -*- coding: utf-8 -*-
"""
02_generate_scores.py —— 对当日因子数据做推理打分
====================================================
输入:
  - 因子面板 parquet(默认取 tasks/factor_engineering/output/factors_YYYYMMDD.parquet 最新,
    或 --panel 指定; 若给全历史面板 factors_panel_full.parquet 则取每只最后一行=最新交易日)
  - 模型(通过 01_load_model.load_model)
模型特征可能含策略信号特征(breakout/pullback/regime), 这些不在因子面板里 ->
若缺失, 则从 harness ctx 现算并合并(与生产 daily_inference 链路一致)。
输出:
  - tasks/model_inference/output/scores_YYYYMMDD.csv (全部股票打分, 含 pred 与特征)
"""
import sys
import glob
import importlib.util
from pathlib import Path

import pandas as pd

SCRIPT = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT))
_spec = importlib.util.spec_from_file_location('load_model_mod', SCRIPT / '01_load_model.py')
_lm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lm)

ROOT = Path('.').resolve()
for p in [str(ROOT / 'tools'), str(ROOT / 'opt_study'), str(ROOT.parent)]:
    if p not in sys.path:
        sys.path.insert(0, p)
try:
    import momentum
except ImportError:
    s = importlib.util.spec_from_file_location('momentum', ROOT / '__init__.py',
                                               submodule_search_locations=[str(ROOT)])
    m = importlib.util.module_from_spec(s)
    sys.modules['momentum'] = m
    s.loader.exec_module(m)
import volume_price_scan as VPS
import volume_price_strategy as VS


def latest_panel():
    files = sorted(glob.glob('tasks/factor_engineering/output/factors_20*.parquet'))
    daily = [f for f in files if 'panel_full' not in f]
    if daily:
        return daily[-1], 'daily'
    if files:
        return files[-1], 'full'
    return None, None


def _load_ctx_features():
    """加载 ctx + 现有策略信号/环境特征(供补全模型输入)。"""
    H = VPS._load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    return ctx, cal, names, hot_at, regime


def generate_scores(panel_path=None, model_path=None):
    mp, kind = (panel_path, 'given') if panel_path else latest_panel()
    if mp is None:
        raise SystemExit('未找到因子面板, 请先运行 factor_pipeline.py')
    print(f'[infer] 面板={mp} ({kind})')
    pan = pd.read_parquet(mp)
    # 统一取每只股票最新一行(=最新交易日), 得到"当日可投 universe"用于推荐
    pan = pan.sort_values(['code', 'trade_date']).groupby('code').tail(1)
    pan['td'] = pan['trade_date'].dt.strftime('%Y-%m-%d') \
        if hasattr(pan['trade_date'], 'dt') else pan['trade_date'].astype(str)

    model, feats, target, meta = _lm.load_model(model_path)
    missing = [f for f in feats if f not in pan.columns]
    if missing:
        print(f'[infer] 模型需要策略特征 {missing}, 从 ctx 现算 ...')
        ctx, cal, names, hot_at, regime = _load_ctx_features()
        inv = VS.build_inv(ctx, cal, names, hot_at, regime)
        bo = {(c, ts) for ts, cs in inv['breakout'].items() for c in cs}
        pb = {(c, ts) for ts, cs in inv['pullback'].items() for c in cs}
        bo_df = pd.DataFrame(list(bo), columns=['code', 'td']); bo_df['breakout'] = 1
        pb_df = pd.DataFrame(list(pb), columns=['code', 'td']); pb_df['pullback'] = 1
        pan = pan.merge(bo_df, on=['code', 'td'], how='left')
        pan = pan.merge(pb_df, on=['code', 'td'], how='left')
        pan[['breakout', 'pullback']] = pan[['breakout', 'pullback']].fillna(0).astype(int)
        reg_df = pd.DataFrame([(ts, r) for ts, r in regime.items()], columns=['td', 'regime_s'])
        reg_map = {'bull': 1, 'ranging': 0, 'bear': -1}
        reg_df['regime'] = reg_df['regime_s'].map(reg_map).fillna(0).astype(int)
        pan = pan.merge(reg_df[['td', 'regime']], on='td', how='left')
        pan['regime'] = pan['regime'].fillna(0).astype(int)

    pan = pan.dropna(subset=feats)
    pan = pan[pan['close'] >= 1.5]
    pan['pred'] = model.predict(pan[feats])
    date = pd.to_datetime(pan['trade_date']).max().strftime('%Y%m%d')
    OUT = Path('tasks/model_inference/output')
    OUT.mkdir(parents=True, exist_ok=True)
    out = pan[['code', 'trade_date', 'close', 'pred'] + feats].copy()
    out = out.sort_values('pred', ascending=False)
    out.to_csv(OUT / f'scores_{date}.csv', index=False)
    print(f'[infer] 打分股票数={len(out)} 输出={OUT / f"scores_{date}.csv"}')
    return out, date, meta


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--panel', default=None)
    ap.add_argument('--model', default=None)
    a = ap.parse_args()
    generate_scores(a.panel, a.model)
