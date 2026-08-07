# -*- coding: utf-8 -*-
"""
04_backtest_entry_exit.py —— 模型 Top-K 买卖时点回测 + 与 volume-price-scan 同期对比
=====================================================================================
回答三个问题:
  1. 模型选出来的票, 买入/卖出时点怎么选最优? (含涨停不可买入的现实约束)
  2. 收益率/胜率与现有规则策略 volume-price-scan 相比如何?
  3. 值不值得实盘?

口径(严格样本外):
  - 模型 model_v1 用 <=2025-12-31 训练(train+val), 本回测只用 test 段 2026-01-01~2026-06-30。
  - 2026-07-01 起本地 DB 全市场数据残缺(07-16 起仅 28 只在更新), 故截止 06-30。

买入时点(entry):
  - close_same : 信号日收盘价买入(理想上界, 实盘不可得, 仅作参考)
  - open_next  : 次日开盘价买入(现实基准)
  - dip_next   : 次日回踩买入, 目标价=信号日收盘*(1-dip_buf), 次日最低触及才成交, 否则放弃

不可买入过滤(现实约束):
  - 信号日涨停(涨幅>=涨跌停幅*0.95) -> 次日大概率高开/一字, 排除
  - 次日一字板(high==low 且涨停) -> 无法买入, 跳过

卖出时点(exit):
  - hold      : 持有 N 个交易日后收盘卖
  - tp_sl     : 止盈+tp / 止损-sl / cap 日到期收盘卖
  - pressure  : 触及前60日最高*0.98 卖出(价量策略已验证有效) + 止损 + cap 到期

输出:
  tasks/model_inference/output/backtest_entry_exit.json / .md
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

OUT = ROOT / 'tasks' / 'model_inference' / 'output'
PANEL = ROOT / 'tasks' / 'factor_engineering' / 'output' / 'factors_panel_full.parquet'
MODEL_TXT = ROOT / 'tasks' / 'model_training' / 'models' / 'model_v1.txt'
META = ROOT / 'tasks' / 'model_training' / 'models' / 'model_meta.json'

TEST_START = '2026-01-01'
TEST_END = '2026-06-30'      # 07-01 起 DB 残缺
COST = 0.0035                # 往返成本: 佣金+印花税+过户费+滑点 ~0.35%


# ---------------------------------------------------------------- 工具
def limit_ratio(code):
    """涨跌停幅度: 创业板(30)/科创板(68) 20%, 其余 10%。"""
    return 0.20 if str(code).startswith(('30', '68')) else 0.10


def is_limit_up(g, i):
    """第 i 根是否涨停(收盘涨幅 >= 幅度*0.95)。"""
    if i <= 0:
        return False
    prev = g['close'].iat[i - 1]
    if prev <= 0:
        return False
    pct = g['close'].iat[i] / prev - 1.0
    return pct >= limit_ratio(g.attrs.get('code', '')) * 0.95


def is_one_word(g, i):
    """一字板: 最高=最低 且 涨停(买不进)。"""
    if i <= 0:
        return False
    return (g['high'].iat[i] == g['low'].iat[i]) and is_limit_up(g, i)


# ---------------------------------------------------------------- 单笔模拟
def simulate_one(g, i, entry, dip_buf, exit_rule, hold_n, tp, sl, cap,
                 skip_limit_up=True):
    """对 g 的第 i 根(信号日)模拟一笔交易, 返回 (ret, reason) 或 None(未成交)。"""
    n = len(g)
    if i + 1 >= n:
        return None

    # --- 现实约束: 信号日已涨停 -> 次日难以买入
    if skip_limit_up and is_limit_up(g, i):
        return None

    sig_close = g['close'].iat[i]

    # --- 买入
    if entry == 'close_same':
        j = i
        buy = sig_close
    elif entry == 'open_next':
        j = i + 1
        if is_one_word(g, j):          # 次日一字板买不进
            return None
        buy = g['open'].iat[j]
    elif entry == 'dip_next':
        j = i + 1
        target = sig_close * (1 - dip_buf)
        if g['low'].iat[j] > target:   # 未回踩到位, 放弃
            return None
        buy = min(target, g['open'].iat[j])   # 若低开则按开盘价成交(更优)
    else:
        raise ValueError(entry)
    if buy <= 0 or not np.isfinite(buy):
        return None

    # --- 压力位(信号日之前 60 根的最高价)
    lo = max(0, i - 60)
    pressure_px = g['high'].iloc[lo:i + 1].max() * 0.98

    # --- 卖出扫描
    end = min(n - 1, j + (hold_n if exit_rule == 'hold' else cap))
    for k in range(j + 1, end + 1):
        hi, lw, cl = g['high'].iat[k], g['low'].iat[k], g['close'].iat[k]
        if exit_rule == 'hold':
            continue
        if sl and lw <= buy * (1 - sl):
            return (buy * (1 - sl) / buy - 1.0 - COST, 'stop_loss')
        if tp and hi >= buy * (1 + tp):
            return (tp - COST, 'take_profit')
        if exit_rule == 'pressure' and cl >= pressure_px:
            return (cl / buy - 1.0 - COST, 'pressure')
    # 到期收盘卖
    return (g['close'].iat[end] / buy - 1.0 - COST, 'expire')


def stats(rets):
    """按笔统计。"""
    if not rets:
        return dict(n=0, win_rate=0.0, avg_ret=0.0, med_ret=0.0,
                    total_ret=0.0, sharpe=0.0, max_dd=0.0)
    a = np.array(rets, dtype=float)
    eq = np.cumprod(1 + a)
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    return dict(
        n=len(a),
        win_rate=float((a > 0).mean()),
        avg_ret=float(a.mean()),
        med_ret=float(np.median(a)),
        total_ret=float(eq[-1] - 1.0),
        sharpe=float(a.mean() / a.std() * np.sqrt(252 / 20)) if a.std() > 1e-12 else 0.0,
        max_dd=float(dd.min()),
    )


# ---------------------------------------------------------------- 数据
def load_ctx():
    H = VPS._load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    for c, g in ctx.items():
        g.attrs['code'] = c
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    return ctx, cal, names, hot_at, regime


def model_signals(ctx, cal, names, hot_at, regime, topk=10):
    """用模型对 test 段每个交易日打分, 取 Top-K, 返回 [(code, date_str)]。"""
    import lightgbm as lgb
    meta = json.loads(META.read_text())
    feats = meta['features']
    booster = lgb.Booster(model_file=str(MODEL_TXT))

    pan = pd.read_parquet(PANEL)
    pan['trade_date'] = pd.to_datetime(pan['trade_date'])
    pan = pan[(pan['trade_date'] >= TEST_START) & (pan['trade_date'] <= TEST_END)].copy()
    pan['td'] = pan['trade_date'].dt.strftime('%Y-%m-%d')

    # 补 breakout/pullback/regime(与生产推理链路一致)
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

    sigs = []
    for td, grp in pan.groupby('td'):
        top = grp.nlargest(topk, 'pred')
        sigs.extend([(str(r['code']), td) for _, r in top.iterrows()])
    return sigs, pan


def vp_signals(ctx, cal, names, hot_at, regime, pullback_lowvol=False, vol_thr=0.5):
    """volume-price-scan 规则策略在 test 段的信号。"""
    vol_pct_at = VS.build_vol_pct(ctx, cal) if pullback_lowvol else None
    inv = VS.build_inv(ctx, cal, names, hot_at, regime,
                       pullback_lowvol=pullback_lowvol,
                       vol_pct_at=vol_pct_at, pullback_vol_thr=vol_thr)
    out = {}
    for kind in ('breakout', 'pullback'):
        s = []
        for ts, cs in inv[kind].items():
            if TEST_START <= ts <= TEST_END:
                s.extend([(c, ts) for c in cs])
        out[kind] = s
    return out


def run_set(sigs, ctx, entry, exit_rule, hold_n=20, tp=None, sl=0.08, cap=20,
            dip_buf=0.02, skip_limit_up=True):
    rets = []
    for code, td in sigs:
        g = ctx.get(code)
        if g is None or td not in g.index:
            continue
        # 注入 code: is_limit_up/limit_ratio 依赖 attrs['code'] 区分 10%/20% 涨跌停幅度。
        # ctx 的 DataFrame 默认 attrs 为空 -> 创业板(30)/科创板(68) 被误按 10% 判涨停,
        # 使涨幅 9.5%~19% 的正常创业板票被误剔除(样本损失, 偏保守)。
        g.attrs['code'] = code
        i = g.index.get_loc(td)
        if isinstance(i, slice):
            continue
        r = simulate_one(g, i, entry, dip_buf, exit_rule, hold_n, tp, sl, cap,
                         skip_limit_up)
        if r is not None:
            rets.append(r[0])
    return stats(rets)


def main():
    print('[1/4] 载入 ctx ...', flush=True)
    ctx, cal, names, hot_at, regime = load_ctx()
    print(f'      ctx={len(ctx)} 只, 日历 {cal[0]}~{cal[-1]}', flush=True)

    print('[2/4] 模型打分 + Top-K 信号 ...', flush=True)
    msigs, pan = model_signals(ctx, cal, names, hot_at, regime, topk=10)
    print(f'      模型信号 {len(msigs)} 笔 ({len(set(t for _, t in msigs))} 个交易日)', flush=True)

    print('[3/4] 价量规则信号 ...', flush=True)
    vp033 = vp_signals(ctx, cal, names, hot_at, regime, pullback_lowvol=True, vol_thr=0.33)
    vp050 = vp_signals(ctx, cal, names, hot_at, regime, pullback_lowvol=True, vol_thr=0.50)
    vpoff = vp_signals(ctx, cal, names, hot_at, regime, pullback_lowvol=False)
    print(f"      breakout={len(vpoff['breakout'])} "
          f"pullback(off)={len(vpoff['pullback'])} "
          f"pullback(0.33)={len(vp033['pullback'])} "
          f"pullback(0.50)={len(vp050['pullback'])}", flush=True)

    print('[4/4] 回测 ...', flush=True)
    res = {'meta': {'test_start': TEST_START, 'test_end': TEST_END, 'cost': COST,
                    'topk': 10}}

    # --- A. 模型: 买入时点对比(卖出统一 hold20)
    res['A_entry'] = {}
    for entry in ('close_same', 'open_next', 'dip_next'):
        for slu in (True, False):
            key = f'{entry}{"" if slu else "_incl_limitup"}'
            res['A_entry'][key] = run_set(msigs, ctx, entry, 'hold', hold_n=20,
                                          skip_limit_up=slu)

    # --- B. 模型: 卖出规则对比(买入统一 open_next)
    res['B_exit'] = {}
    for name, kw in [
        ('hold_5', dict(exit_rule='hold', hold_n=5)),
        ('hold_10', dict(exit_rule='hold', hold_n=10)),
        ('hold_20', dict(exit_rule='hold', hold_n=20)),
        ('tp10_sl8_cap20', dict(exit_rule='tp_sl', tp=0.10, sl=0.08, cap=20)),
        ('tp15_sl8_cap20', dict(exit_rule='tp_sl', tp=0.15, sl=0.08, cap=20)),
        ('pressure_sl8_cap20', dict(exit_rule='pressure', tp=None, sl=0.08, cap=20)),
    ]:
        res['B_exit'][name] = run_set(msigs, ctx, 'open_next', **kw)

    # --- C. 同期对比 volume-price-scan(同买卖口径: open_next + hold20)
    res['C_compare'] = {
        'model_top10': run_set(msigs, ctx, 'open_next', 'hold', hold_n=20),
        'vp_breakout': run_set(vpoff['breakout'], ctx, 'open_next', 'hold', hold_n=20),
        'vp_pullback_off': run_set(vpoff['pullback'], ctx, 'open_next', 'hold', hold_n=20),
        'vp_pullback_033': run_set(vp033['pullback'], ctx, 'open_next', 'hold', hold_n=20),
        'vp_pullback_050': run_set(vp050['pullback'], ctx, 'open_next', 'hold', hold_n=20),
    }

    # --- D. 价量策略在其"最优卖点"(压力位)下的表现, 与模型同规则对照
    res['D_compare_pressure'] = {
        'model_top10': run_set(msigs, ctx, 'dip_next', 'pressure', sl=0.05, cap=20),
        'vp_breakout': run_set(vpoff['breakout'], ctx, 'dip_next', 'pressure', sl=0.08, cap=20),
        'vp_pullback_033': run_set(vp033['pullback'], ctx, 'dip_next', 'pressure', sl=0.05, cap=20),
        'vp_pullback_050': run_set(vp050['pullback'], ctx, 'dip_next', 'pressure', sl=0.05, cap=20),
    }

    # --- E. 基准: 同期全市场等权 20 日收益(信号无关)
    bench = []
    tds = sorted({t for _, t in msigs})
    for td in tds[::5]:
        for code, g in list(ctx.items())[::7]:
            if td not in g.index:
                continue
            i = g.index.get_loc(td)
            if isinstance(i, slice) or i + 21 >= len(g):
                continue
            bench.append(g['close'].iat[i + 20] / g['close'].iat[i + 1] - 1.0)
    res['E_benchmark_market'] = stats(bench)

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'backtest_entry_exit.json').write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))
    print(f'[ok] -> {OUT / "backtest_entry_exit.json"}')


if __name__ == '__main__':
    main()
