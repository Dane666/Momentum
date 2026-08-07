# -*- coding: utf-8 -*-
"""
vp_lowvol_threshold_sweep.py —— 缩量回踩「低波动确认」阈值扫描
================================================================
背景: 生产 volume-price-scan 已开启 VP_PULLBACK_LOWVOL=1, 阈值 VP_PULLBACK_VOL_THR=0.33
      (只保留 vol60 横截面最低 33% 分位的票)。用户希望放宽到 0.5 让信号更密。

本脚本在**全可用窗口**扫描阈值 0.20/0.33/0.50/0.70/1.00(=不过滤),
用价量策略已验证的最优买卖点(次日回踩低吸 dip_buf=0.02 + 压力位卖出 cap=20 + 止损-5%),
比较信号数 / 胜率 / 平均单笔 / 累计。

注: 2026-07-01 起本地 DB 全市场数据残缺(07-16 后仅 28 只在更新), 故窗口截至 2026-06-30。
"""
import sys
import json
import importlib.util
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
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

WIN_START = '2024-07-01'
WIN_END = '2026-06-30'
COST = 0.0035
DIP_BUF = 0.02
SL = 0.05
CAP = 20


def simulate(g, i, dip_buf=DIP_BUF, sl=SL, cap=CAP):
    """次日回踩买入 + 压力位卖出(前60日高*0.98) + 止损 + cap 到期。"""
    n = len(g)
    if i + 1 >= n:
        return None
    sig_close = g['close'].iat[i]
    j = i + 1
    target = sig_close * (1 - dip_buf)
    if g['low'].iat[j] > target:
        return None                      # 未回踩, 不成交
    buy = min(target, g['open'].iat[j])
    if not np.isfinite(buy) or buy <= 0:
        return None
    lo = max(0, i - 60)
    pressure = g['high'].iloc[lo:i + 1].max() * 0.98
    end = min(n - 1, j + cap)
    for k in range(j + 1, end + 1):
        if g['low'].iat[k] <= buy * (1 - sl):
            return buy * (1 - sl) / buy - 1.0 - COST
        if g['close'].iat[k] >= pressure:
            return g['close'].iat[k] / buy - 1.0 - COST
    return g['close'].iat[end] / buy - 1.0 - COST


def stats(rets):
    if not rets:
        return dict(n=0, win_rate=0.0, avg_ret=0.0, sum_ret=0.0, sharpe=0.0)
    a = np.array(rets, dtype=float)
    return dict(n=len(a), win_rate=float((a > 0).mean()),
                avg_ret=float(a.mean()), sum_ret=float(a.sum()),
                sharpe=float(a.mean() / a.std() * np.sqrt(252 / CAP))
                if a.std() > 1e-12 else 0.0)


def main():
    print('[1/3] 载入 ctx ...', flush=True)
    H = VPS._load_harness()
    ctx = H.load_kline(); ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)

    print('[2/3] 预计算 vol_pct ...', flush=True)
    vol_pct_at = VS.build_vol_pct(ctx, cal)

    print('[3/3] 阈值扫描 ...', flush=True)
    res = {}
    for thr in (0.20, 0.33, 0.50, 0.70, 1.01):
        lowvol = thr < 1.0
        inv = VS.build_inv(ctx, cal, names, hot_at, regime,
                           pullback_lowvol=lowvol, vol_pct_at=vol_pct_at,
                           pullback_vol_thr=thr)
        rets = []
        for ts, cs in inv['pullback'].items():
            if not (WIN_START <= ts <= WIN_END):
                continue
            for c in cs:
                g = ctx.get(c)
                if g is None or ts not in g.index:
                    continue
                i = g.index.get_loc(ts)
                if isinstance(i, slice):
                    continue
                r = simulate(g, i)
                if r is not None:
                    rets.append(r)
        raw_n = sum(len(cs) for ts, cs in inv['pullback'].items()
                    if WIN_START <= ts <= WIN_END)
        st = stats(rets)
        st['raw_signals'] = raw_n
        st['fill_rate'] = round(st['n'] / raw_n, 3) if raw_n else 0.0
        label = 'off(不过滤)' if thr > 1.0 else f'{thr:.2f}'
        res[label] = st
        print(f"      thr={label:12s} 原始信号={raw_n:4d} 成交={st['n']:4d} "
              f"胜率={st['win_rate']*100:5.1f}% 均笔={st['avg_ret']*100:+6.2f}% "
              f"累计={st['sum_ret']*100:+7.2f}% 夏普={st['sharpe']:5.2f}", flush=True)

    out = ROOT / 'opt_study' / 'vp_lowvol_threshold_sweep.json'
    out.write_text(json.dumps({'window': [WIN_START, WIN_END], 'result': res},
                              indent=2, ensure_ascii=False))
    print(f'[ok] -> {out}')


if __name__ == '__main__':
    main()
