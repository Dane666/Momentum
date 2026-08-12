# -*- coding: utf-8 -*-
"""
05_trade_records_backtest.py —— 市场状态自适应策略 · 逐笔实盘回测
====================================================================
把「LightGBM 模型通道 + 市场状态自适应仓位」当作一个可实盘执行的策略, 做
逐笔(trade-by-trade)回测, 输出**每一条买入与卖出记录**, 并据此评估实盘可操作性
(买点/卖点/仓位/并发/胜率/回撤)。

与 04 的区别:
  - 04 只算组合净值(夏普/回撤), 不产生逐笔记录。
  - 本脚本模拟真实账户: 每个信号日, 模型给出排名候选; 按当日市场状态决定
    "目标持仓槽位数" = round(并发上限 * 该状态 position_scale); 次日开盘买入,
    持有 10 个交易日或触及 -8% 止损时卖出。每笔都记录买卖明细。

交易规则(对齐部署口径, model_inference_report.md §6.7):
  - 买点: 信号日 T 收盘后已知候选, 于 T+1 开盘价买入(隔夜挂涨停价参与集合竞价,
          实际成交价=开盘价; 次日一字涨停板集合竞价封死无法成交, 已剔除)。
  - 卖点: 满足任一即卖 —— (a) 持有满 10 个交易日收盘清仓; (b) 任一持有日收盘
          <= 买入价*(1-8%) 触发止损, 当日收盘卖出。不叠加止盈(横截面 alpha
          叠加形态卖点净负贡献, 已验证)。
  - 仓位: 滚动仓位池, 并发上限 MAX_POS(默认 5, 与 position_sizing.MAX_HOLDINGS 一致)。
          逆境(high_vol/trend_down)目标槽位少 -> 自然留现金; 顺势(trend_up)满槽。
  - 成本: 双边 0.35‰(与 04 一致), 买入价*(1+cost) 为实际建仓成本。

状态来源: 全 A 等权净值 nav(覆盖完整历史, 与 market_timing/risk_gate 一致),
用 RegimeDetector 判定四态, 再映射到 regime_config.yaml 的 adaptive 段
position_scale。

输出:
  tasks/market_state/trades_adaptive.csv       逐笔交易(自适应策略, 主交付)
  tasks/market_state/trades_baseline.csv       逐笔交易(固定权重基线, 对照)
  tasks/market_state/trade_backtest_result.json 汇总指标
  tasks/market_state/trade_backtest_report.md   可读报告(含样本记录 + 可操作性结论)

运行: python tasks/market_state/05_trade_records_backtest.py
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
at_mod = _load('adaptive_threshold_mod', 'tasks/market_state/03_adaptive_threshold.py')
RegimeDetector = rd.RegimeDetector
load_index_series = rd.load_index_series
AdaptiveThreshold = at_mod.AdaptiveThreshold

_spec = importlib.util.spec_from_file_location(
    'bt', ROOT / 'tasks' / 'model_inference' / '05_portfolio_backtest.py')
bt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bt)

# 复用 04 的候选池构建(含模型分 + 21 特征 + 涨停过滤)
m04 = _load('m04', 'tasks/market_state/04_regime_backtest_compare.py')

TEST_START = '2026-01-01'
TEST_END = '2026-06-30'
COST = 0.0035
STOP = 0.08
HOLD_N = 10
CAPITAL = 1_000_000.0      # 名义本金, 用于把比例换算成可操作的金额
MAX_POS = 5                # 滚动仓位池并发上限(= position_sizing.MAX_HOLDINGS)


def limit_ratio(code):
    """按代码判定涨跌停幅度(与项目其它模块一致): 创业板/科创板 20%, 其余 10%。"""
    return 0.20 if str(code).startswith(('30', '68')) else 0.10


def _build_hl_frames(ctx, idx):
    """构建 high/low 宽表(index=idx, columns=code), 用于"次日一字板"判定。
    逻辑对齐 bt.build_price_frames 的 idx 与过滤口径。"""
    highs, lows = {}, {}
    for c, g in ctx.items():
        sub = g.reindex(idx)
        if sub['close'].notna().sum() < 30:
            continue
        highs[c] = sub['high'].values
        lows[c] = sub['low'].values
    return pd.DataFrame(highs, index=idx), pd.DataFrame(lows, index=idx)


# --------------------------------------------------------------------------- #
# 滚动仓位池 · 逐笔模拟
# --------------------------------------------------------------------------- #
def simulate_trades(picks_by_day, state_of, close_df, open_df, high_df, low_df,
                    date_ts, idx, at, label='adaptive'):
    """模拟逐笔交易, 返回 (trades:list[dict], equity_series:pd.Series, daily_capital:list).

    picks_by_day: {date_str: {'codes':[...], 'scale':float}}  (已按状态选定)
    state_of:     {date_str: state}
    滚动池: 并发上限 MAX_POS, 每日目标槽位 = round(MAX_POS * scale)。
            仅当槽位空闲且未达目标时才开新仓(逆境自然留现金)。
    每笔: 信号日 T 次日开盘买入 -> 持有 HOLD_N 日或 -8% 止损卖出。
    """
    pos = {}                # (signal_day, code) -> position dict
    trades = []
    idx_pos = {d: i for i, d in enumerate(idx)}
    daily_ret = []
    daily_active = []
    peak_concurrent = 0
    one_word_skipped = 0      # 次日一字涨停板(集合竞价封死/无卖盘) -> 挂涨停隔夜单也无法成交

    for d in list(date_ts.keys()):
        ts = date_ts[d]
        di = idx_pos[ts]

        # --- 1) 标记在仓: 计算当日收益, 更新 last_px/held ---
        rets_today = []
        for key, b in pos.items():
            cur = close_df.loc[ts, [b['code']]].values.astype(float)
            prev = b['last_px']
            if np.isfinite(cur) and np.isfinite(prev) and prev > 0:
                rets_today.append(cur / prev - 1.0)
            b['last_px'] = cur if np.isfinite(cur) else prev
            b['held'] += 1
        peak_concurrent = max(peak_concurrent, len(pos))
        port_r = float(np.mean(rets_today)) if rets_today else 0.0
        daily_ret.append((ts, port_r))
        daily_active.append(len(pos))

        # --- 2) 退出判定(用当日收盘) ---
        for key, b in list(pos.items()):
            cp = close_df.loc[ts, [b['code']]].values.astype(float)[0]
            if not np.isfinite(cp):
                continue
            reason = None
            if cp <= b['stop_px']:
                reason = 'stop_loss'
            elif b['held'] >= HOLD_N:
                reason = 'expiry'
            elif di >= len(idx) - 1:
                reason = 'window_end'
            if reason:
                buy_fill = b['entry'] * (1 + COST)
                sell_fill = cp * (1 - COST)
                ret = sell_fill / buy_fill - 1.0
                trades.append(dict(
                    buy_date=b['buy_date'], sell_date=str(ts.date()),
                    code=b['code'], name=b['name'], state=b['state'],
                    scale=round(b['scale'], 2),
                    buy_price=round(b['entry'], 3),
                    sell_price=round(float(cp), 3),
                    hold_days=b['held'], ret_pct=round(ret * 100, 2),
                    capital=round(b['capital'], 0),
                    pnl=round(b['capital'] * ret, 0),
                    reason=reason))
                del pos[key]

        # --- 3) 新开仓(滚动池: 仅补空闲槽位且未达目标) ---
        rec = picks_by_day.get(d)
        if not rec or not rec.get('codes'):
            continue
        st = state_of.get(d, 'range')
        scale = float(rec.get('scale', 1.0))
        codes = rec['codes']
        target = int(round(MAX_POS * scale))
        free = MAX_POS - len(pos)
        n_open = max(0, min(free, target - len(pos), len(codes)))
        if n_open <= 0:
            continue
        nd = idx[di + 1] if di + 1 < len(idx) else None
        if nd is None:
            continue
        opens = open_df.loc[nd, codes].astype(float)
        highs = high_df.loc[nd, codes].astype(float)
        lows = low_df.loc[nd, codes].astype(float)
        prev_close = close_df.loc[ts, codes].astype(float)
        for code in codes[:n_open]:
            px = opens.get(code, np.nan)
            if not np.isfinite(px) or px <= 0:
                continue
            # 次日一字涨停板(open=high=low 且涨幅≥限幅): 集合竞价即封死、无卖盘,
            # 隔夜涨停限价单也无法成交 -> 视为不可买入(与"挂涨停隔夜单"真实口径对齐)。
            pc = prev_close.get(code, np.nan)
            if np.isfinite(pc) and pc > 0:
                up = px / pc - 1.0
                if (abs(highs.get(code, np.nan) - lows.get(code, np.nan)) < 1e-6
                        and abs(opens.get(code, np.nan) - highs.get(code, np.nan)) < 1e-6
                        and up >= limit_ratio(code) * 0.95):
                    one_word_skipped += 1
                    continue
            per_slot = CAPITAL / MAX_POS
            pos[(d, code)] = dict(
                code=code, name=rec.get('names', {}).get(code, code),
                state=st, scale=scale, entry=float(px), last_px=float(px),
                held=0, stop_px=float(px) * (1 - STOP),
                capital=per_slot, buy_date=str(nd.date()))

    eq = pd.Series(dict(daily_ret))
    return trades, eq, daily_active, one_word_skipped


def perf_from_equity(eq: pd.Series):
    s = eq.dropna()
    if len(s) == 0:
        return dict(days=0)
    eqc = (1 + s).cumprod()
    peak = eqc.cummax()
    dd = (eqc / peak - 1.0).min()
    ann = eqc.iat[-1] ** (252 / len(s)) - 1.0
    sharpe = s.mean() / s.std() * np.sqrt(252) if s.std() > 1e-12 else 0.0
    return dict(days=int(len(s)), total_ret=float(eqc.iat[-1] - 1.0),
                ann_ret=float(ann), sharpe=float(sharpe), max_dd=float(dd),
                win_days=float((s > 0).mean()))


def trade_stats(trades):
    if not trades:
        return {}
    rets = np.array([t['ret_pct'] for t in trades]) / 100.0
    wins = rets[rets > 0]; losses = rets[rets <= 0]
    gross_win = wins.sum(); gross_loss = -losses.sum()
    pf = float(gross_win / gross_loss) if gross_loss > 0 else float('inf')
    # 最大连亏
    max_cons = cur = 0
    for r in rets:
        if r <= 0:
            cur += 1; max_cons = max(max_cons, cur)
        else:
            cur = 0
    stop = sum(1 for t in trades if t['reason'] == 'stop_loss')
    expir = sum(1 for t in trades if t['reason'] == 'expiry')
    return dict(
        n=len(trades),
        win_rate=float((rets > 0).mean()),
        avg_win=float(wins.mean()) if len(wins) else 0.0,
        avg_loss=float(losses.mean()) if len(losses) else 0.0,
        profit_factor=pf,
        max_consec_loss=int(max_cons),
        stopped=int(stop), expired=int(expir),
        avg_hold=float(np.mean([t['hold_days'] for t in trades])),
        total_pnl=float(sum(t['pnl'] for t in trades)),
    )


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def main():
    cfg = load_regime_config()
    at = AdaptiveThreshold(cfg)

    print('[1/5] 加载 ctx / 价格宽表 / 候选池(含模型分)', flush=True)
    H = VPS._load_harness()
    ctx = H.load_kline(); ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    env_regime = VS.build_regime(cal, nav)

    # 全样本市场状态(nav 代理) -> state_map
    nav_df = pd.DataFrame({'trade_date': nav.index, 'close': nav.values})
    det = RegimeDetector(cfg.get('detector', {}))
    full_state = det.detect_index(nav_df)
    full_state.index = pd.to_datetime(full_state.index)
    state_map = {d.strftime('%Y-%m-%d'): s for d, s in full_state.items()}

    close_df, open_df, dates, idx = bt.build_price_frames(ctx, cal)
    high_df, low_df = _build_hl_frames(ctx, idx)   # 用于"次日一字板"剔除
    dates = [d for d in dates if TEST_START <= str(d.date()) <= TEST_END]
    cands, feats = m04.build_candidates(ctx, cal, names, hot_at, env_regime)
    print(f'      回测交易日 {len(dates)} 天; 候选池每日均值 '
          f'{int(np.mean([len(v) for v in cands.values()]))} 只', flush=True)

    del ctx, H, hot_at, nav
    gc.collect()

    date_ts = {str(d.date()): d for d in dates}
    state_of = {d: state_map.get(d, 'range') for d in date_ts}

    # 组装 picks_by_day: 自适应(按状态门槛筛选 + 仓位) vs 固定(满仓 top5)
    adapt_picks, base_picks = {}, {}
    for d, grp in cands.items():
        st = state_of[d]
        # 自适应: 模型分排序 + 状态门槛 + 仓位(收缩暴露)
        sel = at.select(grp, 'model_pred', state=st)
        if not sel.empty:
            scale = at.params(st).get('position_scale', 1.0)
            adapt_picks[d] = {'codes': list(sel['code']),
                              'scale': scale, 'names': names}
        # 固定基线: 满仓 top5, scale=1
        top = grp.nlargest(5, 'model_pred')
        base_picks[d] = {'codes': list(top['code']), 'scale': 1.0, 'names': names}

    print('[2/5] 逐笔模拟(滚动仓位池, 并发上限=%d)' % MAX_POS, flush=True)
    tr_adv, eq_adv, cap_adv, skip_adv = simulate_trades(
        adapt_picks, state_of, close_df, open_df, high_df, low_df, date_ts, idx, at, 'adaptive')
    tr_base, eq_base, cap_base, skip_base = simulate_trades(
        base_picks, state_of, close_df, open_df, high_df, low_df, date_ts, idx, at, 'baseline')
    print(f'      自适应交易 {len(tr_adv)} 笔; 固定基线 {len(tr_base)} 笔', flush=True)

    pa = perf_from_equity(eq_adv); pb = perf_from_equity(eq_base)
    sa = trade_stats(tr_adv); sb = trade_stats(tr_base)

    # 可操作性: 并发/资本
    peak_conc = max(cap_adv) if cap_adv else 0
    avg_conc = float(np.mean(cap_adv)) if cap_adv else 0
    avg_scale = float(np.mean([adapt_picks[d]['scale'] for d in adapt_picks]))
    cash_pct = 1 - avg_scale

    regime_days = {k: int(v) for k, v in
                   pd.Series([state_of[d] for d in date_ts]).value_counts().items()}

    result = {
        'window': f'{TEST_START}~{TEST_END}',
        'max_pos': MAX_POS, 'hold_n': HOLD_N, 'stop': STOP, 'cost': COST,
        'capital': CAPITAL,
        'adaptive': {'equity': pa, 'trades': sa,
                     'peak_concurrent': peak_conc, 'avg_concurrent': round(avg_conc, 2)},
        'baseline': {'equity': pb, 'trades': sb},
        'regime_days': regime_days,
        'avg_position_scale': round(avg_scale, 3),
        'cash_reserve_pct': round(cash_pct * 100, 1),
        'one_word_skipped': {'adaptive': int(skip_adv), 'baseline': int(skip_base)},
        'accept_operable': 'YES' if peak_conc <= MAX_POS else 'NO',
    }
    OUT = ROOT / 'tasks' / 'market_state'
    (OUT / 'trade_backtest_result.json').write_text(
        json.dumps(result, indent=2, ensure_ascii=False))
    pd.DataFrame(tr_adv).to_csv(OUT / 'trades_adaptive.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(tr_base).to_csv(OUT / 'trades_baseline.csv', index=False, encoding='utf-8-sig')

    # 控制台摘要
    def line(tag, eq, tr):
        return (f'{tag}: 夏普={eq["sharpe"]:+.3f} 总收益={eq["total_ret"]*100:>+7.2f}% '
                f'回撤={eq["max_dd"]*100:>6.2f}% | 笔数={tr["n"]} '
                f'胜率={tr["win_rate"]*100:>5.1f}% 盈亏比PF={tr["profit_factor"]:.2f} '
                f'均持={tr["avg_hold"]:.1f}日 止损={tr["stopped"]}({tr["stopped"]/tr["n"]*100:.0f}%)')
    print('\n=== 逐笔实盘回测(滚动池 并发%d, %s~%s) ===' % (MAX_POS, TEST_START, TEST_END))
    print(line('自适应(模型+状态仓位)', pa, sa))
    print(line('固定基线(满仓top5)  ', pb, sb))
    print(f'\n市场状态分布: {regime_days}')
    print(f'次日一字板剔除(集合竞价封死/挂涨停单也买不进): 自适应 {skip_adv} 笔, 基线 {skip_base} 笔')
    print(f'自适应平均仓位比例={avg_scale:.2f} -> 平均留现金 {cash_pct*100:.1f}%')
    print(f'并发持仓峰值={peak_conc} (上限{MAX_POS}) -> 可手动管理: '
          f'{"是" if peak_conc<=MAX_POS else "否"}')
    print(f'[ok] -> trades_adaptive.csv / trades_baseline.csv / trade_backtest_result.json')


if __name__ == '__main__':
    main()
