# -*- coding: utf-8 -*-
"""
07_exit_rule_study.py —— 模型信号的卖点规则研究（固定持有 vs 压力位/均线/价量）
============================================================================
背景: 06 已实证买点应固定为「信号次日开盘价」(可买 85.7%, 跳空中位数 0.00%,
      回踩限价单存在逆向选择)。买点既已锁定, 剩下的收益弹性全在**卖点**。

本脚本只变卖点, 买点一律 open_next, 回答:
  「持有10日到期」是不是太钝? 压力位 / 均线 / 价量关系 卖出能不能更好?

两层验证(缺一不可):
  Layer 1 按笔  —— 单笔信号质量, 快速横扫全部卖出规则。
  Layer 2 组合  —— **可变持有期的滚动资金池**(资金 1.0, 最多同时持 M 只,
                   卖出释放资金后立刻补新票)。这才对应"实盘滚动", 因为
                   早卖 = 资金早周转 = 能多打一轮, 按笔统计看不出这个收益。

窗口: 2026-01-01 ~ 2026-06-30(模型训练截止 2025-12-31, 严格样本外)。
成本: 双边 0.35%。
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
TEST_END = '2026-06-30'
COST = 0.0035          # 双边合计, 买卖各扣一半

# ⚠️ 数据完整性红线: 本地 DB 自 2026-07-01 起全市场残缺
#    (07-01 仅 2337 只 / 07-15 仅 2042 只 / 07-29 起仅 28 只, 正常应 ~2950 只)。
#    任何持仓/净值一旦延伸进这段, 结果全部失真(实测会把 hold_5 从真实的负收益
#    伪造成 +21%)。因此: 所有交易强制在 TEST_END 收盘了结, 净值只在 test 段内计算。


def limit_ratio(code):
    return 0.20 if str(code).startswith(('30', '68')) else 0.10


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


def enrich(ctx):
    """为卖点规则预计算辅助列(只用当日及之前的信息, 无前视)。"""
    for c, g in ctx.items():
        if len(g) < 5:
            continue
        # 压力位: 截至"前一日"的 60 日最高价(信号日当天不参与, 避免用当日高点)
        g['press60'] = g['high'].rolling(60, min_periods=20).max().shift(1)
        g['ma10'] = g['close'].rolling(10, min_periods=5).mean()
        # 价量: 放量(≥2×5日均量)且收阴/滞涨 -> 主升段衰竭信号
        vma5 = g['volume'].rolling(5, min_periods=3).mean()
        g['vol_spike'] = g['volume'] >= 2.0 * vma5
        g['bear_bar'] = g['close'] <= g['open']
    return ctx


def is_limit_up(g, i):
    if i <= 0:
        return False
    prev = g['close'].iat[i - 1]
    if not np.isfinite(prev) or prev <= 0:
        return False
    pct = g['close'].iat[i] / prev - 1.0
    return pct >= limit_ratio(g.attrs.get('code', '')) * 0.95


def is_one_word(g, j):
    o, h, l = g['open'].iat[j], g['high'].iat[j], g['low'].iat[j]
    return abs(h - l) < 1e-9 and abs(o - h) < 1e-9


# ---------------------------------------------------------------- 卖出规则
# 每条规则: 给定持仓上下文, 返回 (sell_price, reason) 或 None
def make_rule(kind, hold_n=10, sl=0.08, cap=20, trail=0.07, trail_arm=0.05):
    """构造卖出判定器。

    判定顺序统一为: 止损 > 止盈类(压力位/移动止盈) > 形态类(均线/价量) > 到期。
    止损用当日 low 触发(悲观), 其余用收盘价成交(可执行)。
    """
    def rule(g, k, buy_px, held_days, peak_px, press_px):
        hi, lw, cl = g['high'].iat[k], g['low'].iat[k], g['close'].iat[k]

        # 1) 止损(所有规则通用, 除非 sl=None)
        if sl and lw <= buy_px * (1 - sl):
            return (buy_px * (1 - sl), 'stop_loss')

        # 2) 纯到期规则
        if kind == 'hold':
            if held_days >= hold_n:
                return (cl, 'expire')
            return None

        # 3) 压力位: 盘中触及前60日高*0.98 即卖(挂单可成交)
        if kind in ('pressure', 'pressure_or_ma10', 'pressure_or_trail'):
            if press_px and np.isfinite(press_px) and hi >= press_px:
                return (max(press_px, g['open'].iat[k]), 'pressure')

        # 4) 移动止盈: 盈利达 trail_arm 后启用, 自持仓最高点回撤 trail 卖出
        if kind in ('trail', 'pressure_or_trail'):
            if peak_px >= buy_px * (1 + trail_arm):
                stop = peak_px * (1 - trail)
                if lw <= stop:
                    return (min(stop, g['open'].iat[k]), 'trail_stop')

        # 5) 均线破位: 收盘跌破均线
        ma_col = {'ma5_break': 'ma5', 'ma10_break': 'ma10', 'ma20_break': 'ma20'}.get(kind)
        if kind == 'pressure_or_ma10':
            ma_col = 'ma10'
        if ma_col:
            mv = g[ma_col].iat[k]
            if np.isfinite(mv) and cl < mv:
                return (cl, f'{ma_col}_break')

        # 6) 价量衰竭: 放量滞涨(量≥2×5日均量 且 收阴)
        if kind in ('volsig', 'pressure_or_volsig'):
            if bool(g['vol_spike'].iat[k]) and bool(g['bear_bar'].iat[k]):
                return (cl, 'vol_exhaust')
        if kind == 'pressure_or_volsig':
            if press_px and np.isfinite(press_px) and hi >= press_px:
                return (max(press_px, g['open'].iat[k]), 'pressure')

        # 7) 持有上限
        if held_days >= cap:
            return (cl, 'cap')
        return None
    return rule


RULES = {
    # --- 基线: 固定持有
    'hold_5':            dict(kind='hold', hold_n=5,  sl=None),
    'hold_10':           dict(kind='hold', hold_n=10, sl=None),
    'hold_20':           dict(kind='hold', hold_n=20, sl=None),
    'hold_10_sl8':       dict(kind='hold', hold_n=10, sl=0.08),
    # --- 压力位
    'pressure_sl8':      dict(kind='pressure', sl=0.08, cap=20),
    'pressure_sl5':      dict(kind='pressure', sl=0.05, cap=20),
    'pressure_cap10':    dict(kind='pressure', sl=0.08, cap=10),
    # --- 均线破位
    'ma5_break_sl8':     dict(kind='ma5_break',  sl=0.08, cap=20),
    'ma10_break_sl8':    dict(kind='ma10_break', sl=0.08, cap=20),
    'ma20_break_sl8':    dict(kind='ma20_break', sl=0.08, cap=20),
    # --- 价量关系(放量滞涨衰竭)
    'volsig_sl8':        dict(kind='volsig', sl=0.08, cap=20),
    'volsig_cap10':      dict(kind='volsig', sl=0.08, cap=10),
    'volsig_cap30':      dict(kind='volsig', sl=0.08, cap=30),
    'volsig_nosl':       dict(kind='volsig', sl=None, cap=20),
    # --- 移动止盈
    'trail7_sl8':        dict(kind='trail', sl=0.08, cap=20, trail=0.07, trail_arm=0.05),
    'trail10_sl8':       dict(kind='trail', sl=0.08, cap=20, trail=0.10, trail_arm=0.05),
    # --- 组合
    'press_or_ma10_sl8': dict(kind='pressure_or_ma10',  sl=0.08, cap=20),
    'press_or_trail_sl8': dict(kind='pressure_or_trail', sl=0.08, cap=20,
                              trail=0.07, trail_arm=0.05),
    'press_or_vol_sl8':  dict(kind='pressure_or_volsig', sl=0.08, cap=20),
}


# ---------------------------------------------------------------- Layer 1 按笔
def simulate_one(g, i, rule_kw, stop_i=None):
    """信号日 i, 次日开盘买入, 按 rule 卖出。返回 (ret, reason, hold_days) 或 None。

    stop_i: 该股 TEST_END 对应的行号。持仓不得延伸到其之后(那段数据残缺),
            到点未了结则按 stop_i 收盘强制平仓。
    """
    n = len(g)
    if i + 1 >= n:
        return None
    if is_limit_up(g, i):
        return None
    j = i + 1
    if is_one_word(g, j):
        return None
    buy = g['open'].iat[j]
    if not np.isfinite(buy) or buy <= 0:
        return None

    press_px = g['press60'].iat[i] * 0.98 if 'press60' in g.columns else np.nan
    cap = rule_kw.get('cap', rule_kw.get('hold_n', 20))
    rule = make_rule(**rule_kw)

    peak = buy
    end = min(n - 1, j + cap)
    if stop_i is not None:
        end = min(end, stop_i)
    if end <= j:
        return None
    for k in range(j, end + 1):
        peak = max(peak, g['high'].iat[k])
        if k == j:            # 买入当日不卖(避免同日进出的不可执行假设)
            continue
        out = rule(g, k, buy, k - j, peak, press_px)
        if out:
            px, reason = out
            return (px / buy - 1.0 - COST, reason, k - j)
    # 未触发任何规则: 到达 cap 或撞上窗口末端
    truncated = stop_i is not None and end == stop_i and end < j + cap
    return (g['close'].iat[end] / buy - 1.0 - COST,
            'window_end' if truncated else 'cap', end - j)


def stats_trades(recs):
    if not recs:
        return dict(n=0, win_rate=0.0, avg_ret=0.0, med_ret=0.0,
                    sharpe=0.0, avg_hold=0.0, reasons={})
    a = np.array([r[0] for r in recs], dtype=float)
    hd = np.array([r[2] for r in recs], dtype=float)
    reasons = {}
    for r in recs:
        reasons[r[1]] = reasons.get(r[1], 0) + 1
    # 按笔夏普按平均持有期折算成年
    per_year = 252.0 / max(1.0, hd.mean())
    # 去尾均值: 砍掉两端各 5%, 检验收益是否被极少数极端值撑起来
    lo, hi = np.percentile(a, [5, 95])
    trimmed = a[(a >= lo) & (a <= hi)]
    return dict(
        n=len(a),
        win_rate=float((a > 0).mean()),
        avg_ret=float(a.mean()),
        med_ret=float(np.median(a)),
        trimmed_avg=float(trimmed.mean()) if len(trimmed) else 0.0,
        sharpe=float(a.mean() / a.std() * np.sqrt(per_year)) if a.std() > 1e-12 else 0.0,
        avg_hold=float(hd.mean()),
        reasons=reasons,
    )


# ---------------------------------------------------------------- 选股
def model_picks_by_day(ctx, cal, names, hot_at, regime, topn=20):
    """返回 {Timestamp: [code 按分数降序]} —— 多取一些, 组合层按空位截取。"""
    import lightgbm as lgb
    meta = json.loads(META.read_text())
    feats = meta['features']
    booster = lgb.Booster(model_file=str(MODEL_TXT))

    pan = pd.read_parquet(PANEL)
    pan['trade_date'] = pd.to_datetime(pan['trade_date'])
    pan = pan[(pan['trade_date'] >= TEST_START) & (pan['trade_date'] <= TEST_END)].copy()
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

    # 涨停过滤(次日买不进)
    keep = []
    for code, td in zip(pan['code'].astype(str), pan['td']):
        g = ctx.get(code)
        if g is None or td not in g.index:
            keep.append(False); continue
        i = g.index.get_loc(td)
        if isinstance(i, slice) or i <= 0:
            keep.append(False); continue
        prev = g['close'].iat[i - 1]
        pct = g['close'].iat[i] / prev - 1.0 if prev > 0 else 0.0
        keep.append(pct < limit_ratio(code) * 0.95)
    pan = pan[pd.Series(keep, index=pan.index)]

    out = {}
    for td, grp in pan.groupby('td'):
        out[pd.Timestamp(td)] = [str(c) for c in
                                 grp.nlargest(topn, 'pred')['code'].tolist()]
    return out


# ---------------------------------------------------------------- Layer 2 组合
def simulate_pool(picks_by_day, ctx, dates, rule_kw, max_pos=5):
    """可变持有期的滚动资金池。

    每个交易日 d:
      ① 先按卖出规则清算持仓(释放资金)
      ② 再用**前一日**信号在 d 的开盘价补仓, 直到持满 max_pos
    资金等权: 每笔投入 = 当前总权益 / max_pos(受现金约束)。
    """
    rule = make_rule(**rule_kw)
    cap = rule_kw.get('cap', rule_kw.get('hold_n', 20))

    cash = 1.0
    pos = []            # dict(code, shares, buy_px, buy_k, peak, press)
    nav, trades = [], []
    last_px = {}        # 停牌时用最后已知价估值
    last_k = len(dates) - 1

    for k, d in enumerate(dates):
        # ---- ① 卖出
        for p in list(pos):
            g = ctx.get(p['code'])
            if g is None or d not in g.index:
                continue                        # 停牌: 持有不动
            kk = g.index.get_loc(d)
            if isinstance(kk, slice):
                continue
            p['peak'] = max(p['peak'], g['high'].iat[kk])
            held = k - p['buy_k']
            if held < 1:
                continue
            out = rule(g, kk, p['buy_px'], held, p['peak'], p['press'])
            if out is None and held >= cap:
                out = (g['close'].iat[kk], 'cap')
            if out:
                px, reason = out
                cash += p['shares'] * px * (1 - COST / 2)
                trades.append(dict(code=p['code'], ret=px / p['buy_px'] - 1.0 - COST,
                                   hold=held, reason=reason))
                pos.remove(p)

        # ---- ②' 窗口末日: 强制平仓(其后数据残缺, 不可延续)
        if k == last_k:
            for p in list(pos):
                g = ctx.get(p['code'])
                px = last_px.get(p['code'], p['buy_px'])
                if g is not None and d in g.index:
                    kk = g.index.get_loc(d)
                    if not isinstance(kk, slice):
                        px = g['close'].iat[kk]
                cash += p['shares'] * px * (1 - COST / 2)
                trades.append(dict(code=p['code'], ret=px / p['buy_px'] - 1.0 - COST,
                                   hold=k - p['buy_k'], reason='window_end'))
                pos.remove(p)
            nav.append(cash)
            break

        # ---- ② 买入(用前一日信号, 今日开盘执行)
        if k >= 1 and len(pos) < max_pos:
            held_codes = {p['code'] for p in pos}
            # 权益按前一日收盘估
            equity = cash + sum(pp['shares'] * last_px.get(pp['code'], pp['buy_px'])
                                for pp in pos)
            unit = equity / max_pos
            for code in picks_by_day.get(dates[k - 1], []):
                if len(pos) >= max_pos:
                    break
                if code in held_codes:
                    continue
                g = ctx.get(code)
                if g is None or d not in g.index:
                    continue
                kk = g.index.get_loc(d)
                if isinstance(kk, slice) or kk <= 0:
                    continue
                if is_one_word(g, kk):
                    continue
                px = g['open'].iat[kk]
                if not np.isfinite(px) or px <= 0:
                    continue
                alloc = min(unit, cash)
                if alloc < unit * 0.5:          # 现金不足半个仓位, 不勉强开
                    break
                shares = alloc * (1 - COST / 2) / px
                sig_i = kk - 1
                press = g['press60'].iat[sig_i] * 0.98 if 'press60' in g.columns else np.nan
                pos.append(dict(code=code, shares=shares, buy_px=px, buy_k=k,
                                peak=px, press=press))
                held_codes.add(code)
                cash -= alloc

        # ---- ③ 估值
        mv = 0.0
        for p in pos:
            g = ctx.get(p['code'])
            if g is not None and d in g.index:
                kk = g.index.get_loc(d)
                if not isinstance(kk, slice):
                    last_px[p['code']] = g['close'].iat[kk]
            mv += p['shares'] * last_px.get(p['code'], p['buy_px'])
        nav.append(cash + mv)

    return np.array(nav, dtype=float), trades


def perf(nav, dates):
    if len(nav) < 2:
        return dict(total_ret=0.0, annual=0.0, sharpe=0.0, max_dd=0.0, day_win=0.0)
    r = np.diff(nav) / nav[:-1]
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    years = len(nav) / 252.0
    return dict(
        total_ret=float(nav[-1] / nav[0] - 1.0),
        annual=float((nav[-1] / nav[0]) ** (1 / years) - 1.0) if years > 0 else 0.0,
        sharpe=float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 1e-12 else 0.0,
        max_dd=float(dd.min()),
        day_win=float((r > 0).mean()),
    )


def market_benchmark(ctx, dates):
    """全市场等权 buy&hold。"""
    rets = []
    for k in range(1, len(dates)):
        d0, d1 = dates[k - 1], dates[k]
        vals = []
        for g in ctx.values():
            if d0 in g.index and d1 in g.index:
                a, b = g['close'].get(d0), g['close'].get(d1)
                if a and b and a > 0:
                    vals.append(b / a - 1.0)
        rets.append(np.mean(vals) if vals else 0.0)
    nav = np.cumprod(1 + np.array([0.0] + rets))
    return nav


# ---------------------------------------------------------------- main
def main():
    print('[load] ctx ...', flush=True)
    ctx, cal, names, hot_at, regime = load_ctx()
    ctx = enrich(ctx)

    print('[signals] model picks ...', flush=True)
    picks = model_picks_by_day(ctx, cal, names, hot_at, regime, topn=20)
    print(f'  交易日 {len(picks)} 天, 平均候选 '
          f'{np.mean([len(v) for v in picks.values()]):.1f} 只', flush=True)

    # 每只票在 TEST_END 的行号 —— 持仓不得越过它(其后数据残缺)
    ts_end = pd.Timestamp(TEST_END)
    stop_idx = {}
    for c, g in ctx.items():
        p = g.index.searchsorted(ts_end, side='right') - 1
        if p >= 0:
            stop_idx[c] = int(p)

    # ---- Layer 1: 按笔。Top5 / Top10 两个口径(Top5 与组合层可比)
    layer1 = {}
    for tag, topn in (('top5', 5), ('top10', 10)):
        sigs = []
        for td, codes in picks.items():
            for c in codes[:topn]:
                g = ctx.get(c)
                if g is None or td not in g.index:
                    continue
                i = g.index.get_loc(td)
                if not isinstance(i, slice):
                    sigs.append((c, i))
        print(f'[layer1-{tag}] 信号 {len(sigs)} 笔, 横扫 {len(RULES)} 条规则 ...',
              flush=True)
        sub, raw = {}, {}
        for name, kw in RULES.items():
            recs, rmap = [], {}
            for c, i in sigs:
                r = simulate_one(ctx[c], i, kw, stop_i=stop_idx.get(c))
                if r:
                    recs.append(r)
                    rmap[(c, i)] = r[0]
            sub[name] = stats_trades(recs)
            raw[name] = rmap
            s = sub[name]
            print(f'  {name:20s} n={s["n"]:4d} 胜率{s["win_rate"]:6.1%} '
                  f'均笔{s["avg_ret"]:+7.2%} 去尾{s["trimmed_avg"]:+7.2%} '
                  f'持有{s["avg_hold"]:4.1f}日 夏普{s["sharpe"]:5.2f}', flush=True)
        layer1[tag] = sub
        if tag == 'top5':
            raw_top5 = raw
        else:
            raw_top10 = raw

    # ---- Layer 3: 配对显著性检验(同一批信号, 只换卖点 -> 可配对)
    from scipy import stats as sps
    print('[layer3] 配对 t 检验 (基准 = hold_10, 同信号配对) ...', flush=True)
    layer3 = {}
    for tag, raw in (('top5', raw_top5), ('top10', raw_top10)):
        base = raw['hold_10']
        row = {}
        for name, rmap in raw.items():
            if name == 'hold_10':
                continue
            keys = sorted(set(base) & set(rmap))
            if len(keys) < 30:
                continue
            d = np.array([rmap[k] - base[k] for k in keys], dtype=float)
            nz = d[np.abs(d) > 1e-12]          # 有效差异笔数(规则未触发时两者相同)
            t, p = sps.ttest_rel([rmap[k] for k in keys], [base[k] for k in keys])
            row[name] = dict(n_pairs=len(keys), n_diff=int(len(nz)),
                             mean_diff=float(d.mean()), t_stat=float(t),
                             p_value=float(p),
                             better=int((nz > 0).sum()), worse=int((nz < 0).sum()))
        layer3[tag] = row
        if tag == 'top5':
            sig = sorted(row.items(), key=lambda kv: kv[1]['p_value'])
            for name, v in sig[:6]:
                mark = '***' if v['p_value'] < 0.01 else (
                    '**' if v['p_value'] < 0.05 else ('*' if v['p_value'] < 0.10 else ''))
                print(f'  {name:20s} Δ均笔{v["mean_diff"]:+7.2%} '
                      f't={v["t_stat"]:+5.2f} p={v["p_value"]:.3f}{mark} '
                      f'(改善{v["better"]}/恶化{v["worse"]}笔)', flush=True)

    # ---- Layer 2: 组合(滚动资金池)。区间严格截止 TEST_END, 不碰残缺数据段
    ts0, ts1 = pd.Timestamp(TEST_START), pd.Timestamp(TEST_END)
    dates = [d for d in cal if ts0 <= d <= ts1]

    pool_rules = ['hold_5', 'hold_10', 'hold_20', 'hold_10_sl8', 'volsig_cap10',
                  'pressure_sl8', 'pressure_cap10', 'ma10_break_sl8', 'ma20_break_sl8',
                  'volsig_sl8', 'trail7_sl8', 'press_or_ma10_sl8',
                  'press_or_trail_sl8', 'press_or_vol_sl8']

    bench = market_benchmark(ctx, dates)
    bench_perf = perf(bench, dates)
    print(f'[layer2] 滚动资金池, {len(dates)} 交易日 (截止 {TEST_END})', flush=True)
    print(f'  [基准] 全市场等权 总收益{bench_perf["total_ret"]:+.2%} '
          f'夏普{bench_perf["sharpe"]:.2f} 回撤{bench_perf["max_dd"]:.2%}', flush=True)

    def run_pool(name, max_pos, dts):
        nav, trades = simulate_pool(picks, ctx, dts, RULES[name], max_pos=max_pos)
        pf = perf(nav, dts)
        rets = np.array([t['ret'] for t in trades]) if trades else np.array([0.0])
        hold = np.array([t['hold'] for t in trades]) if trades else np.array([0.0])
        reasons = {}
        for t in trades:
            reasons[t['reason']] = reasons.get(t['reason'], 0) + 1
        pf.update(n_trades=len(trades),
                  trade_win=float((rets > 0).mean()) if trades else 0.0,
                  trade_avg_ret=float(rets.mean()) if trades else 0.0,
                  avg_hold=float(hold.mean()) if trades else 0.0,
                  turnover_per_year=float(len(trades) / (len(dts) / 252.0)),
                  reasons=reasons)
        return pf

    print(f'  --- 主口径 max_pos=5 ---', flush=True)
    layer2 = {}
    for name in pool_rules:
        pf = run_pool(name, 5, dates)
        pf['excess'] = pf['total_ret'] - bench_perf['total_ret']
        layer2[name] = pf
        print(f'  {name:20s} 总{pf["total_ret"]:+7.2%} 夏普{pf["sharpe"]:5.2f} '
              f'回撤{pf["max_dd"]:7.2%} 超额{pf["excess"]:+7.2%} '
              f'笔{pf["n_trades"]:3d} 均笔{pf["trade_avg_ret"]:+6.2%} '
              f'持有{pf["avg_hold"]:4.1f}日 胜率{pf["trade_win"]:5.1%}', flush=True)

    # ---- 稳健性 A: 换仓位数(结论若只在 max_pos=5 成立, 就是噪声)
    print('  --- 稳健性A: max_pos ∈ {3,8} ---', flush=True)
    robust_pos = {}
    for mp in (3, 8):
        robust_pos[f'max_pos_{mp}'] = {
            name: {k: v for k, v in run_pool(name, mp, dates).items()
                   if k != 'reasons'} for name in pool_rules}
        row = robust_pos[f'max_pos_{mp}']
        best = sorted(row.items(), key=lambda kv: -kv[1]['total_ret'])[:3]
        print(f'    max_pos={mp} 前三: ' +
              ', '.join(f'{n}({v["total_ret"]:+.2%})' for n, v in best), flush=True)

    # ---- 稳健性 B: 分半窗口(H1a / H1b)
    print('  --- 稳健性B: 前后半窗 ---', flush=True)
    mid = len(dates) // 2
    halves = {'first_half': dates[:mid], 'second_half': dates[mid:]}
    robust_half = {}
    for hname, dts in halves.items():
        bh = perf(market_benchmark(ctx, dts), dts)
        robust_half[hname] = {'benchmark': bh}
        for name in pool_rules:
            pf = {k: v for k, v in run_pool(name, 5, dts).items() if k != 'reasons'}
            pf['excess'] = pf['total_ret'] - bh['total_ret']
            robust_half[hname][name] = pf
        best = sorted(((n, v) for n, v in robust_half[hname].items()
                       if n != 'benchmark'), key=lambda kv: -kv[1]['total_ret'])[:3]
        print(f'    {hname}(基准{bh["total_ret"]:+.2%}) 前三: ' +
              ', '.join(f'{n}({v["total_ret"]:+.2%})' for n, v in best), flush=True)

    res = dict(window=[TEST_START, TEST_END], cost=COST, max_pos=5,
               n_days=len(dates), benchmark=bench_perf,
               layer1_per_trade=layer1, layer3_paired=layer3, layer2_pool=layer2,
               robust_max_pos=robust_pos, robust_half=robust_half)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'exit_rule_study.json').write_text(
        json.dumps(res, ensure_ascii=False, indent=2, default=float))
    print(f'\n[done] -> {OUT / "exit_rule_study.json"}')


if __name__ == '__main__':
    main()
