# -*- coding: utf-8 -*-
"""
04_regime_backtest_compare.py —— 固定权重 vs 动态因子权重 回测对比
====================================================================
目标: 验证"动态因子权重 + 市场状态识别"是否提升 LightGBM 模型通道的
      胜率与收益(对照固定权重基线)。

设计要点(诚实、可复现、内存安全):
  1. LightGBM 接入: booster.predict 在 21 维模型特征上打分(其中 breakout/
     pullback/regime 由 ctx 现算, 其余 18 维来自因子面板)。
  2. 市场状态(RegimeDetector, 四态 trend_up/trend_down/range/high_vol)由宽基
     指数(沪深300 优先 / 上证回退)判定, 每个交易日一个状态。
  3. 动态因子权重 = 数据驱动: 用「测试窗口之前」的样本, 按市场状态分组,
     估计每个因子组的横截面 Rank-IC(对 fwd20 前向收益), 得到的状态依赖权重
     是样本外(OOS)的, 不是手拍。
  4. 对比隔离"选股"效应: 动态版保持满仓(与基线同样 top_k / 同等暴露), 只改
     最终排序分 = z(模型分) + blend * 状态倾斜(各组 z-score 按 OOS 权重加权)。
     这样夏普/收益差异纯粹来自"状态感知的重新排序", 而非砍仓。
  5. 同时如实报告自适应阈值版(逆境收缩仓位)——此前发现对横截面 alpha 模型
     在逆向市会丢失 alpha, 故单独列示、不混为一谈。

输出: tasks/market_state/regime_backtest_result.json + 控制台对比表。

运行: python tasks/market_state/04_regime_backtest_compare.py
"""
import sys
import gc
import json
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path('.').resolve()
MS = ROOT / 'tasks' / 'market_state'
for p in [str(ROOT / 'tools'), str(ROOT / 'opt_study'),
          str(ROOT / 'tasks' / 'model_inference'), str(MS)]:
    if p not in sys.path:
        sys.path.insert(0, p)

import volume_price_scan as VPS   # noqa: E402
import volume_price_strategy as VS  # noqa: E402
from _universe import filter_st     # noqa: E402
from config_loader import load_regime_config  # noqa: E402


def _load(name, path):
    s = importlib.util.spec_from_file_location(name, ROOT / path)
    m = importlib.util.module_from_spec(s); sys.modules[name] = m
    s.loader.exec_module(m); return m


rd = _load('regime_detector_mod', 'tasks/market_state/01_regime_detector.py')
dw_mod = _load('dynamic_weights_mod', 'tasks/market_state/02_dynamic_weights.py')
at_mod = _load('adaptive_threshold_mod', 'tasks/market_state/03_adaptive_threshold.py')
RegimeDetector = rd.RegimeDetector
load_index_series = rd.load_index_series
DynamicWeights = dw_mod.DynamicWeights
AdaptiveThreshold = at_mod.AdaptiveThreshold

# 复用 05 的价格模拟框架
_spec = importlib.util.spec_from_file_location(
    'bt', ROOT / 'tasks' / 'model_inference' / '05_portfolio_backtest.py')
bt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bt)

TEST_START = '2026-01-01'
TEST_END = '2026-06-30'
IC_TRAIN_START = '2023-01-01'   # 权重估计只用测试窗口之前的样本(OOS)
COST = 0.0035
TOPK = 5
HOLD_N = 10

# 18 个可由因子面板直接提供的模型特征 -> 4 个可解释因子组(用于状态倾斜)
# (breakout/pullback/regime 由 ctx 现算, 已含在模型分里, 倾斜不再重复叠加)
TILT_FEATURES = ['hist_vol_60', 'bias_60', 'cmf_20', 'boll_pctb_20', 'boll_width_20',
                 'obv', 'atr_pct_14', 'kdj_j', 'hist_vol_ratio', 'macd_hist_slope',
                 'obv_ma_ratio', 'main_flow_ratio', 'mfi_14', 'kdj_d', 'turn_rank_60',
                 'turn_ma_ratio', 'turn_zscore_20', 'turn_chg_5']
TILT_GROUPS = {
    'momentum':   ['bias_60', 'macd_hist_slope', 'boll_pctb_20', 'kdj_j', 'kdj_d'],
    'volatility': ['hist_vol_60', 'hist_vol_ratio', 'atr_pct_14', 'boll_width_20'],
    'money_flow': ['cmf_20', 'obv', 'obv_ma_ratio', 'main_flow_ratio', 'mfi_14'],
    'liquidity':  ['turn_rank_60', 'turn_ma_ratio', 'turn_zscore_20', 'turn_chg_5'],
}
STATES = ('trend_up', 'trend_down', 'range', 'high_vol')


# --------------------------------------------------------------------------- #
# 数据驱动权重估计: 每个市场状态下, 各因子组对 fwd20 的横截面 Rank-IC
# --------------------------------------------------------------------------- #
def estimate_regime_weights(panel_path, state_map, train_start=IC_TRAIN_START,
                            sample_codes_frac=0.4):
    """用测试窗口之前的样本, 估计 state -> {group: ic_weight}。

    panel: 含 code/trade_date/close + TILT_FEATURES。state_map: {date_str: state}。
    返回 weights: dict[state] -> dict[group] -> float(该组特征 IC 均值)。
    """
    cols = ['code', 'trade_date', 'close'] + TILT_FEATURES
    pan = pd.read_parquet(panel_path, columns=cols)
    pan['trade_date'] = pd.to_datetime(pan['trade_date'])
    pan = pan[(pan['trade_date'] >= train_start) &
              (pan['trade_date'] < TEST_START)].copy()
    # 抽样 code(保留每只 code 的连续序列, fwd20 的 shift 才正确)
    rng = np.random.default_rng(42)
    codes = pan['code'].unique()
    keep = rng.choice(codes, size=max(50, int(len(codes) * sample_codes_frac)),
                      replace=False)
    pan = pan[pan['code'].isin(keep)].copy()
    print(f'    [IC估计] 训练样本 {len(pan):,} 行, {pan.code.nunique()} 只, '
          f'{pan.trade_date.min().date()}~{pan.trade_date.max().date()}', flush=True)

    # 前向 20 日收益(状态依赖权重的目标是它)
    pan = pan.sort_values(['code', 'trade_date'])
    pan['fwd20'] = pan.groupby('code')['close'].transform(
        lambda x: x.shift(-20) / x - 1.0)
    pan['mstate'] = pan['trade_date'].dt.strftime('%Y-%m-%d').map(state_map)
    pan = pan.dropna(subset=['fwd20'] + TILT_FEATURES + ['mstate'])
    if len(pan) < 1000:
        print('    [IC估计] 样本不足, 退回等权默认', flush=True)
        return None

    # 每日横截面标准化, 再跨日汇总(池化横截面 IC)
    zcols = {}
    for f in TILT_FEATURES + ['fwd20']:
        zcols[f + '_z'] = (pan.groupby('trade_date')[f]
                    .transform(lambda x: (x - x.mean()) / (x.std(ddof=0) + 1e-9)))
    pan = pd.concat([pan, pd.DataFrame(zcols, index=pan.index)], axis=1)

    weights = {}
    for st in STATES:
        sub = pan[pan['mstate'] == st]
        w = {}
        for g, fl in TILT_GROUPS.items():
            ics = []
            for f in fl:
                zf = f + '_z'; zt = 'fwd20_z'
                if zf not in sub or zt not in sub:
                    continue
                a = sub[zf].values; b = sub[zt].values
                m = np.isfinite(a) & np.isfinite(b)
                if m.sum() < 50:
                    continue
                c = np.corrcoef(a[m], b[m])[0, 1]
                if np.isfinite(c):
                    ics.append(c)
            w[g] = float(np.mean(ics)) if ics else 0.0
        weights[st] = w
    return weights


# --------------------------------------------------------------------------- #
# 候选池(含模型分 + 21 特征, 供动态重排); 与基线共用同一 universe
# --------------------------------------------------------------------------- #
def limit_ratio(code):
    return 0.20 if str(code).startswith(('30', '68')) else 0.10


def build_candidates(ctx, cal, names, hot_at, env_regime):
    import lightgbm as lgb
    META = ROOT / 'tasks' / 'model_training' / 'models' / 'model_meta.json'
    PANEL = ROOT / 'tasks' / 'factor_engineering' / 'output' / 'factors_panel_full.parquet'
    MODEL_TXT = ROOT / 'tasks' / 'model_training' / 'models' / 'model_v1.txt'
    meta = json.loads(META.read_text())
    feats = meta['features']
    booster = lgb.Booster(model_file=str(MODEL_TXT))

    pan = pd.read_parquet(PANEL)
    pan['trade_date'] = pd.to_datetime(pan['trade_date'])
    pan = pan[(pan['trade_date'] >= TEST_START) &
              (pan['trade_date'] <= TEST_END)].copy()
    pan = filter_st(pan)
    pan['td'] = pan['trade_date'].dt.strftime('%Y-%m-%d')

    inv = VS.build_inv(ctx, cal, names, hot_at, env_regime)
    bo = {(c, ts) for ts, cs in inv['breakout'].items() for c in cs}
    pb = {(c, ts) for ts, cs in inv['pullback'].items() for c in cs}
    bo_df = pd.DataFrame(list(bo), columns=['code', 'td']); bo_df['breakout'] = 1
    pb_df = pd.DataFrame(list(pb), columns=['code', 'td']); pb_df['pullback'] = 1
    pan = pan.merge(bo_df, on=['code', 'td'], how='left')
    pan = pan.merge(pb_df, on=['code', 'td'], how='left')
    pan[['breakout', 'pullback']] = pan[['breakout', 'pullback']].fillna(0).astype(int)
    reg_map = {'bull': 1, 'ranging': 0, 'bear': -1}
    reg_df = pd.DataFrame([(ts, reg_map.get(r, 0)) for ts, r in env_regime.items()],
                          columns=['td', 'regime'])
    pan = pan.merge(reg_df, on='td', how='left')
    pan['regime'] = pan['regime'].fillna(0).astype(int)
    pan = pan.dropna(subset=feats)
    pan = pan[pan['close'] >= 1.5]
    pan['model_pred'] = booster.predict(pan[feats].values)

    # 涨停过滤(信号日涨幅 >= 幅度*0.95 排除, 次日买不进)
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
        out[td] = grp.copy()   # key = 'YYYY-MM-DD' 字符串
    return out, feats


# --------------------------------------------------------------------------- #
# 组合模拟(支持每篮仓位比例 scale; 满仓 baseline 用 scale=1)
# --------------------------------------------------------------------------- #
def simulate(picks_by_day, close_df, open_df, date_ts, idx, hold_n=HOLD_N):
    pos = {}
    daily_ret = []
    idx_pos = {d: i for i, d in enumerate(idx)}

    for di, d in enumerate(dates := list(date_ts.keys())):
        ts = date_ts[d]
        rets_today = []
        for bid, b in list(pos.items()):
            i = idx_pos[ts]
            cur = close_df.loc[ts, b['codes']].values.astype(float)
            prev = b['last_px']
            valid = np.isfinite(cur) & np.isfinite(prev) & (prev > 0)
            r = float(np.mean(cur[valid] / prev[valid] - 1.0)) if valid.sum() else 0.0
            rets_today.append(b['scale'] * r)
            b['last_px'] = np.where(np.isfinite(cur), cur, prev)
            b['held'] += 1
            if b['held'] >= hold_n:
                rets_today[-1] = b['scale'] * (r - COST) if b['scale'] > 0 else 0.0
                del pos[bid]
        port_r = (sum(rets_today) / hold_n) if rets_today else 0.0
        daily_ret.append((ts, port_r))

        rec = picks_by_day.get(d)
        if rec and rec.get('codes') and di + 1 < len(idx):
            nd = idx[di + 1]
            codes = [c for c in rec['codes'] if c in open_df.columns]
            if codes:
                px = open_df.loc[nd, codes].astype(float)
                ok = px[np.isfinite(px.values) & (px.values > 0)]
                if len(ok) > 0:
                    pos[f'{d}'] = dict(codes=list(ok.index),
                                       last_px=ok.values.astype(float),
                                       held=0, scale=float(rec.get('scale', 1.0)))
    return pd.Series(dict(daily_ret))


def basket_returns(picks_by_day, close_df, open_df, date_ts, idx, hold_n=HOLD_N):
    """每篮 10 日收益(次日开盘买, 持有 hold_n 日收盘卖), 用于"交易级胜率"。"""
    idx_pos = {d: i for i, d in enumerate(idx)}
    out = []
    for d, rec in picks_by_day.items():
        if not rec or not rec.get('codes'):
            continue
        ts = date_ts.get(d)
        if ts is None or d not in date_ts:
            continue
        di = idx_pos[ts]
        if di + hold_n >= len(idx):
            continue
        nd = idx[di + 1]
        sd = idx[di + hold_n]
        codes = [c for c in rec['codes'] if c in open_df.columns and c in close_df.columns]
        if not codes:
            continue
        buy = open_df.loc[nd, codes].astype(float)
        sell = close_df.loc[sd, codes].astype(float)
        m = np.isfinite(buy.values) & (buy.values > 0) & np.isfinite(sell.values)
        if m.sum() == 0:
            continue
        ret = float(np.mean(sell.values[m] / buy.values[m] - 1.0))
        out.append(ret - COST * 2)   # 双边成本近似
    return np.array(out)


def perf(series, name):
    s = series.dropna()
    if len(s) == 0:
        return dict(name=name, days=0)
    eq = (1 + s).cumprod()
    peak = eq.cummax()
    dd = (eq / peak - 1.0)
    ann = float(eq.iat[-1] ** (252 / len(s)) - 1.0)
    sharpe = float(s.mean() / s.std() * np.sqrt(252)) if s.std() > 1e-12 else 0.0
    return dict(name=name, days=int(len(s)), total_ret=float(eq.iat[-1] - 1.0),
                ann_ret=ann, sharpe=sharpe, max_dd=float(dd.min()),
                win_days=float((s > 0).mean()))


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def main():
    cfg = load_regime_config()
    at = AdaptiveThreshold(cfg)

    # --- 1) 准备 RegimeDetector(状态标签在 nav 就绪后填充) -------------- #
    print('[1/6] 准备 RegimeDetector', flush=True)
    det = RegimeDetector(cfg.get('detector', {}))
    state_map = None   # 在 [2/6] nav 就绪后用全A等权净值填充(全样本覆盖)

    # --- 2) ctx / 价格 / 候选池 ------------------------------------------ #
    print('[2/6] 加载 ctx / 价格宽表 / 候选池(含模型分)', flush=True)
    H = VPS._load_harness()
    ctx = H.load_kline(); ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    env_regime = VS.build_regime(cal, nav)
    # 用全A等权净值(nav)作为宽基代理判定全样本市场状态: 覆盖测试窗口及 IC 估计
    # 所需的完整历史区间(本地上证指数库残缺, 用 nav 更稳, 与 market_timing/risk_gate 一致)
    nav_df = pd.DataFrame({'trade_date': nav.index, 'close': nav.values})
    full_state = det.detect_index(nav_df)
    full_state.index = pd.to_datetime(full_state.index)
    state_map = {d.strftime('%Y-%m-%d'): s for d, s in full_state.items()}
    close_df, open_df, dates, idx = bt.build_price_frames(ctx, cal)
    dates = [d for d in dates if TEST_START <= str(d.date()) <= TEST_END]
    print(f'      回测交易日 {len(dates)} 天', flush=True)

    cands, feats = build_candidates(ctx, cal, names, hot_at, env_regime)
    print(f'      候选池每日均值 {int(np.mean([len(v) for v in cands.values()]))} 只',
          flush=True)

    # 释放重对象, 避免后面加载 IC 估计面板时 OOM
    del ctx, H, hot_at, nav
    gc.collect()

    # --- 3) 数据驱动权重估计(OOS) ---------------------------------------- #
    print('[3/6] 估计各市场状态的因子组权重(样本外 Rank-IC)', flush=True)
    PANEL = ROOT / 'tasks' / 'factor_engineering' / 'output' / 'factors_panel_full.parquet'
    est = estimate_regime_weights(PANEL, state_map)
    if est is None:
        est = {s: {g: 0.0 for g in TILT_GROUPS} for s in STATES}
    print('    估计权重(因子组 Rank-IC 均值):', flush=True)
    for s in STATES:
        print(f'      {s:10s}: ' + ', '.join(
            f'{g}={est[s][g]:+.3f}' for g in TILT_GROUPS), flush=True)

    # --- 4) 固定权重基线(纯模型分 top_k, 满仓) --------------------------- #
    print('[4/6] 固定权重基线(纯 LightGBM 模型分)', flush=True)
    base_picks = {}
    for d, grp in cands.items():
        top = grp.nlargest(TOPK, 'model_pred')
        base_picks[d] = {'codes': list(top['code'])}

    # --- 5) 两个组件的独立隔离 ------------------------------------------ #
    #  (a) 动态因子权重: 纯模型排序 + 状态感知线性倾斜(满仓) -> 测"因子权重"增益
    #  (b) 市场状态自适应: 纯模型排序 + 状态依赖门槛/仓位(收缩暴露) -> 测"状态识别"增益
    print('[5/6] 动态因子权重(倾斜, 满仓) + 市场状态自适应(模型+仓位)', flush=True)
    dw = DynamicWeights(cfg)
    dw.groups = TILT_GROUPS                 # 仅用面板可提供的 18 特征做倾斜
    dw.weights = est                       # 数据驱动权重覆盖手拍权重
    dw.tilt_blend = 1.0                    # 让倾斜有可比量级(IC 量级较小)
    date_ts = {str(d.date()): d for d in dates}
    state_of = {d: state_map.get(d, 'range') for d in date_ts}

    dyn_picks = {}; adapt_picks = {}
    for d, grp in cands.items():
        st = state_of[d]
        # (a) 因子权重倾斜版: 用 final_score(=z模型 + 状态倾斜) 满仓 top_k
        grp2 = dw.apply(grp, st, model_col='model_pred')
        top = grp2.nlargest(TOPK, 'final_score')
        dyn_picks[d] = {'codes': list(top['code']), 'scale': 1.0}
        # (b) 市场状态自适应版: 仍按"模型分"排序, 但按状态门槛筛选 + 收缩仓位
        sel = at.select(grp, 'model_pred', state=st)
        if sel.empty:
            continue
        scale = at.params(st).get('position_scale', 1.0)
        adapt_picks[d] = {'codes': list(sel['code']), 'scale': scale}

    # --- 6) 模拟 + 对比 -------------------------------------------------- #
    print('[6/6] 组合模拟与对比', flush=True)
    s_base = simulate(base_picks, close_df, open_df, date_ts, idx, HOLD_N)
    s_dyn = simulate(dyn_picks, close_df, open_df, date_ts, idx, HOLD_N)
    s_adp = simulate(adapt_picks, close_df, open_df, date_ts, idx, HOLD_N)
    p_base, p_dyn, p_adp = perf(s_base, 'fixed'), perf(s_dyn, 'dynamic'), perf(s_adp, 'adaptive')

    bench = bt.market_benchmark(close_df, dates)
    p_bench = perf(bench, 'bench')

    br_base = basket_returns(base_picks, close_df, open_df, date_ts, idx, HOLD_N)
    br_dyn = basket_returns(dyn_picks, close_df, open_df, date_ts, idx, HOLD_N)
    br_adp = basket_returns(adapt_picks, close_df, open_df, date_ts, idx, HOLD_N)

    result = {
        'window': f'{TEST_START}~{TEST_END}',
        'top_k': TOPK, 'hold_n': HOLD_N,
        'fixed': p_base, 'dynamic_factor_tilt': p_dyn, 'adaptive_state': p_adp,
        'benchmark_eqw': p_bench,
        'sharpe_delta_factor_tilt': p_dyn['sharpe'] - p_base['sharpe'],
        'sharpe_delta_adaptive': p_adp['sharpe'] - p_base['sharpe'],
        'basket_winrate_fixed': float((br_base > 0).mean()) if len(br_base) else None,
        'basket_winrate_dynamic': float((br_dyn > 0).mean()) if len(br_dyn) else None,
        'basket_winrate_adaptive': float((br_adp > 0).mean()) if len(br_adp) else None,
        'basket_n': int(len(br_base)),
        'accept_factor_tilt_plus_0.2': 'PASS' if (p_dyn['sharpe'] - p_base['sharpe']) >= 0.2 else 'FAIL',
        'accept_adaptive_plus_0.2': 'PASS' if (p_adp['sharpe'] - p_base['sharpe']) >= 0.2 else 'FAIL',
        'regime_days': {k: int(v) for k, v in
                        pd.Series([state_of[d] for d in date_ts]).value_counts().items()},
        'estimated_weights': est,
    }
    out = ROOT / 'tasks' / 'market_state' / 'regime_backtest_result.json'
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False))

    def fmt(p, tag):
        return (f'{tag:<14}夏普={p["sharpe"]:+.3f} 总收益={p["total_ret"]*100:>+7.2f}% '
                f'年化={p["ann_ret"]*100:>+6.2f}% 回撤={p["max_dd"]*100:>6.2f}% '
                f'日胜率={p["win_days"]*100:>5.1f}%')
    print(f'\n=== 回测对比 (top{TOPK}, hold{HOLD_N}, {TEST_START}~{TEST_END}) ===')
    print(fmt(p_bench, '基准等权'))
    print(fmt(p_base, '固定权重(基线)'))
    print(fmt(p_dyn, '动态因子权重'))
    print(fmt(p_adp, '市场状态自适应'))
    def wr(label, v):
        return f'{label}={v*100:.1f}%' if v is not None else f'{label}=n/a'
    print(f'\n交易级胜率(10日篮, n={result["basket_n"]}): '
          f'{wr("固定", result["basket_winrate_fixed"])} | '
          f'{wr("动态因子", result["basket_winrate_dynamic"])} | '
          f'{wr("自适应", result["basket_winrate_adaptive"])}')
    print(f'\n夏普增量(相对固定基线):')
    print(f'  动态因子权重 = {result["sharpe_delta_factor_tilt"]:+.3f} '
          f'-> 验收(+0.2): {result["accept_factor_tilt_plus_0.2"]}')
    print(f'  市场状态自适应 = {result["sharpe_delta_adaptive"]:+.3f} '
          f'-> 验收(+0.2): {result["accept_adaptive_plus_0.2"]}')
    print(f'市场状态分布: {result["regime_days"]}')
    print(f'[ok] -> {out}')


if __name__ == '__main__':
    main()
