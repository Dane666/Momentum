# -*- coding: utf-8 -*-
"""
05_portfolio_backtest.py —— 组合级模拟（净值/夏普/回撤/超额）+ 分月归因
============================================================================
按笔统计（04脚本）只能看单笔信号质量, 不等于实盘账户收益。本脚本做真正的组合模拟:

  资金分 N 份滚动建仓(N=持有期): 每个交易日用 1/N 资金等权买入当日 Top-K,
  持有 N 个交易日后卖出。任一时点有 N 个重叠篮子在场 -> 满仓运行、无择时偏差。

对照基准: 同期全市场等权(相当于"闭眼买一篮子A股")。
关键指标: 年化收益、夏普、最大回撤、**相对基准超额(alpha)**、月度胜负。

弱市里绝对收益为负不代表策略无效 —— 要看是否显著跑赢同期市场。
"""
import sys
import json
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
for p in [str(ROOT / 'tools'), str(ROOT / 'opt_study'), str(ROOT.parent)]:
    if p not in sys.path:
        sys.path.insert(0, p)
try:
    import momentum  # noqa: F401
except ImportError:
    _s = importlib.util.spec_from_file_location(
        'momentum', ROOT / '__init__.py', submodule_search_locations=[str(ROOT)])
    _m = importlib.util.module_from_spec(_s)
    sys.modules['momentum'] = _m
    _s.loader.exec_module(_m)

import volume_price_scan as VPS      # noqa: E402
import volume_price_strategy as VS   # noqa: E402
from _universe import filter_st       # noqa: E402

OUT = ROOT / 'tasks' / 'model_inference' / 'output'
PANEL = ROOT / 'tasks' / 'factor_engineering' / 'output' / 'factors_panel_full.parquet'
MODEL_TXT = ROOT / 'tasks' / 'model_training' / 'models' / 'model_v1.txt'
META = ROOT / 'tasks' / 'model_training' / 'models' / 'model_meta.json'

TEST_START = '2026-01-01'
TEST_END = '2026-06-30'
COST = 0.0035


def limit_ratio(code):
    return 0.20 if str(code).startswith(('30', '68')) else 0.10


def build_price_frames(ctx, cal):
    """把 ctx 转成 close/open 宽表(index=日期, columns=code), 便于向量化。"""
    # 注意: cal 元素是 pd.Timestamp, 而 build_inv/regime 的 key 是字符串, 勿混用
    ts0, ts1 = pd.Timestamp(TEST_START), pd.Timestamp(TEST_END)
    dates = [d for d in cal if ts0 <= d <= ts1]
    # 需要 test 段之后 N 日用于持有期结算 -> 多留 40 个交易日
    after = [d for d in cal if d > ts1][:40]
    idx = dates + after
    closes, opens = {}, {}
    for c, g in ctx.items():
        sub = g.reindex(idx)
        if sub['close'].notna().sum() < 30:
            continue
        closes[c] = sub['close'].values
        opens[c] = sub['open'].values
    close_df = pd.DataFrame(closes, index=idx)
    open_df = pd.DataFrame(opens, index=idx)
    return close_df, open_df, dates, idx


def model_topk_by_day(ctx, cal, names, hot_at, regime, topk=5, skip_limit_up=True):
    import lightgbm as lgb
    meta = json.loads(META.read_text())
    feats = meta['features']
    booster = lgb.Booster(model_file=str(MODEL_TXT))

    pan = pd.read_parquet(PANEL)
    pan['trade_date'] = pd.to_datetime(pan['trade_date'])
    pan = pan[(pan['trade_date'] >= TEST_START) & (pan['trade_date'] <= TEST_END)].copy()
    pan = filter_st(pan)              # 剔除 ST/*ST(戴帽风险, 与实盘候选池一致)
    pan['td'] = pan['trade_date'].dt.strftime('%Y-%m-%d')

    inv = VS.build_inv(ctx, cal, names, hot_at, regime)
    bo = {(c, ts) for ts, cs in inv['breakout'].items() for c in cs}
    pb = {(c, ts) for ts, cs in inv['pullback'].items() for c in cs}
    bo_df = pd.DataFrame(list(bo), columns=['code', 'td']); bo_df['breakout'] = 1
    pb_df = pd.DataFrame(list(pb), columns=['code', 'td']); pb_df['pullback'] = 1
    pan = pan.merge(bo_df, on=['code', 'td'], how='left')
    pan = pan.merge(pb_df, on=['code', 'td'], how='left')
    pan[['breakout', 'pullback']] = pan[['breakout', 'pullback']].fillna(0).astype(int)
    reg_map = {'bull': 1, 'ranging': 0, 'bear': -1}
    reg_df = pd.DataFrame([(ts, reg_map.get(r, 0)) for ts, r in regime.items()],
                          columns=['td', 'regime'])
    pan = pan.merge(reg_df, on='td', how='left')
    pan['regime'] = pan['regime'].fillna(0).astype(int)
    pan = pan.dropna(subset=feats)
    pan = pan[pan['close'] >= 1.5]
    pan['pred'] = booster.predict(pan[feats].values)

    # 涨停过滤: 信号日涨幅 >= 幅度*0.95 的排除(次日买不进)
    if skip_limit_up:
        keep = []
        for _, r in pan.iterrows():
            g = ctx.get(str(r['code']))
            if g is None or r['td'] not in g.index:
                keep.append(False); continue
            i = g.index.get_loc(r['td'])
            if isinstance(i, slice) or i <= 0:
                keep.append(False); continue
            prev = g['close'].iat[i - 1]
            pct = g['close'].iat[i] / prev - 1.0 if prev > 0 else 0.0
            keep.append(pct < limit_ratio(r['code']) * 0.95)
        pan = pan[pd.Series(keep, index=pan.index)]

    out = {}
    for td, grp in pan.groupby('td'):
        # key 统一为 Timestamp, 与 close_df/open_df 的 index 对齐
        out[pd.Timestamp(td)] = [str(c) for c in grp.nlargest(topk, 'pred')['code'].tolist()]
    return out


def simulate_portfolio(picks_by_day, close_df, open_df, dates, idx, hold_n=10):
    """滚动 N 份资金组合模拟, 返回日收益序列(index=日期)。"""
    pos = {}   # basket_id -> dict(codes, buy_px, sell_day_i)
    daily_ret = []
    idx_pos = {d: i for i, d in enumerate(idx)}

    for di, d in enumerate(dates):
        # 当日各在场篮子的收益(用收盘对收盘)
        rets_today = []
        for bid, b in list(pos.items()):
            i = idx_pos[d]
            prev_px = b['last_px']
            cur = close_df.loc[d, b['codes']].values.astype(float)
            valid = np.isfinite(cur) & np.isfinite(prev_px) & (prev_px > 0)
            if valid.sum() == 0:
                r = 0.0
            else:
                r = float(np.mean(cur[valid] / prev_px[valid] - 1.0))
            rets_today.append(r)
            b['last_px'] = np.where(np.isfinite(cur), cur, prev_px)
            b['held'] += 1
            if b['held'] >= hold_n:
                rets_today[-1] = r - COST      # 卖出成本
                del pos[bid]
        # 组合当日收益 = 在场篮子均值 / N 份(未满仓部分为现金 0 收益)
        n_slots = hold_n
        port_r = (sum(rets_today) / n_slots) if rets_today else 0.0
        daily_ret.append((d, port_r))

        # 建新仓: 次日开盘买入 -> 记为从次日起计
        codes = picks_by_day.get(d, [])
        if codes and di + 1 < len(idx):
            nd = idx[di + 1]
            px = open_df.loc[nd, [c for c in codes if c in open_df.columns]] \
                if any(c in open_df.columns for c in codes) else None
            if px is not None and len(px) > 0:
                ok = px[np.isfinite(px.values) & (px.values > 0)]
                if len(ok) > 0:
                    pos[f'{d}'] = dict(codes=list(ok.index),
                                       last_px=ok.values.astype(float),
                                       held=0)
    return pd.Series(dict(daily_ret))


def market_benchmark(close_df, dates):
    """全市场等权日收益。"""
    sub = close_df.loc[dates]
    r = sub.pct_change()
    return r.mean(axis=1).fillna(0.0)


def perf(series, name):
    s = series.dropna()
    if len(s) == 0:
        return dict(name=name, days=0)
    eq = (1 + s).cumprod()
    peak = eq.cummax()
    dd = (eq / peak - 1.0)
    ann = float(eq.iat[-1] ** (252 / len(s)) - 1.0)
    sharpe = float(s.mean() / s.std() * np.sqrt(252)) if s.std() > 1e-12 else 0.0
    return dict(name=name, days=int(len(s)),
                total_ret=float(eq.iat[-1] - 1.0), ann_ret=ann,
                sharpe=sharpe, max_dd=float(dd.min()),
                win_days=float((s > 0).mean()))


def monthly(series):
    s = series.copy()
    s.index = pd.to_datetime(s.index)
    return ((1 + s).resample('ME').prod() - 1.0).round(4)


def main():
    print('[1/4] 载入 ctx ...', flush=True)
    H = VPS._load_harness()
    ctx = H.load_kline(); ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)

    print('[2/4] 构建价格宽表 ...', flush=True)
    close_df, open_df, dates, idx = build_price_frames(ctx, cal)
    print(f'      {len(dates)} 交易日 x {close_df.shape[1]} 只', flush=True)

    print('[3/4] 模型 Top-K ...', flush=True)
    res = {}
    bench = market_benchmark(close_df, dates)
    res['benchmark_market_eqw'] = perf(bench, 'market_eqw')

    for topk in (3, 5, 10):
        picks = model_topk_by_day(ctx, cal, names, hot_at, regime, topk=topk)
        for hold in (5, 10, 20):
            s = simulate_portfolio(picks, close_df, open_df, dates, idx, hold_n=hold)
            key = f'model_top{topk}_hold{hold}'
            p = perf(s, key)
            b = perf(bench, 'bench')
            p['excess_total'] = p['total_ret'] - b['total_ret']
            res[key] = p
            print(f"      {key:22s} 总收益={p['total_ret']*100:+6.2f}% "
                  f"超额={p['excess_total']*100:+6.2f}% 夏普={p['sharpe']:5.2f} "
                  f"回撤={p['max_dd']*100:6.2f}%", flush=True)
            if topk == 5 and hold == 10:
                res['_monthly_model_top5_hold10'] = {
                    str(k.date()): float(v) for k, v in monthly(s).items()}
                res['_monthly_benchmark'] = {
                    str(k.date()): float(v) for k, v in monthly(bench).items()}

    print('[4/4] 输出 ...', flush=True)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'portfolio_backtest.json').write_text(
        json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps({k: v for k, v in res.items() if not k.startswith('_')},
                     indent=2, ensure_ascii=False))
    print(f'[ok] -> {OUT / "portfolio_backtest.json"}')


if __name__ == '__main__':
    main()
