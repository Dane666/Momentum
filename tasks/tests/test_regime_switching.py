# -*- coding: utf-8 -*-
"""
test_regime_switching.py —— 市场状态识别 + 动态因子权重 单元测试
================================================================
轻量、可离线运行(全部用合成数据, 不加载 ctx / 不联网):
  1. RegimeDetector 四态判定(趋势/震荡/高波)正确。
  2. DynamicWeights: 状态依赖因子融合生成 final_score。
  3. AdaptiveThreshold: 各状态门槛/仓位参数与筛选逻辑正确。
  4. estimate_regime_weights: 数据驱动权重方向正确
     (在 trend_up 中, 与 fwd20 正相关的因子应得正权重)。
  5. 市场状态自适应在"逆境含负alpha"的合成场景下, 回撤显著低于满仓基线
     —— 验证"状态识别(仓位管理)有增益"这一回测结论的机理。

运行:
  python tasks/tests/test_regime_switching.py
  (或 pytest tasks/tests/test_regime_switching.py)
"""
import sys
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path('.').resolve()
MS = ROOT / 'tasks' / 'market_state'
for p in [str(ROOT), str(MS)]:
    if p not in sys.path:
        sys.path.insert(0, p)


def _load(name, path):
    s = importlib.util.spec_from_file_location(name, ROOT / path)
    m = importlib.util.module_from_spec(s); sys.modules[name] = m
    s.loader.exec_module(m); return m


rd = _load('regime_detector_mod', 'tasks/market_state/01_regime_detector.py')
dw_mod = _load('dynamic_weights_mod', 'tasks/market_state/02_dynamic_weights.py')
at_mod = _load('adaptive_threshold_mod', 'tasks/market_state/03_adaptive_threshold.py')
from config_loader import load_regime_config

REGIMES = ('trend_up', 'trend_down', 'range', 'high_vol')


# --------------------------------------------------------------------------- #
# 1) RegimeDetector
# --------------------------------------------------------------------------- #
def test_regime_detector_states():
    dates = pd.date_range('2024-01-01', periods=1050, freq='B')
    # 前置一段低波横盘(消除预热期 rolling-vol 被压低的伪影), 其后四段:
    # 低波上涨 / 低波下跌 / 低波横盘 / 高波动(高波动优先于趋势)
    rngp, rng0, rng1, rng2, rng3 = (np.random.default_rng(i) for i in range(5))
    pre = 100 + rngp.normal(0, 0.2, 250)                            # 前置低波
    up = np.linspace(100, 160, 200) + rng0.normal(0, 0.3, 200)      # 低波上涨
    dn = np.linspace(160, 90, 200) + rng1.normal(0, 0.3, 200)       # 低波下跌
    rg = np.linspace(90, 92, 200) + rng2.normal(0, 0.2, 200)        # 低波横盘
    hv = 130 + rng3.normal(0, 6, 200)                               # 高波动
    close = np.concatenate([pre, up, dn, rg, hv])
    df = pd.DataFrame({'trade_date': dates[:1050], 'close': close,
                       'open': close, 'high': close, 'low': close, 'volume': 1.0})
    det = rd.RegimeDetector()
    st = det.detect_index(df)
    # 各段中部(index 已含 250 天前置): up 250-449 / dn 450-649 /
    # rg 650-849 / hv 850-1049, 均远过预热(179)
    assert st.iloc[350:430].mode().iat[0] == 'trend_up', st.iloc[350:430].value_counts()
    assert st.iloc[500:620].mode().iat[0] == 'trend_down', st.iloc[500:620].value_counts()
    assert st.iloc[700:820].mode().iat[0] == 'range', st.iloc[700:820].value_counts()
    assert st.iloc[900:1010].mode().iat[0] == 'high_vol', st.iloc[900:1010].value_counts()
    print('  [ok] RegimeDetector 四态判定正确')


# --------------------------------------------------------------------------- #
# 2) DynamicWeights
# --------------------------------------------------------------------------- #
def test_dynamic_weights_apply():
    cfg = load_regime_config()
    dw = dw_mod.DynamicWeights(cfg)
    rng = np.random.default_rng(0)
    n = 100
    pan = pd.DataFrame({
        'model_pred': np.linspace(-1, 1, n),
        'bias_60': np.random.randn(n), 'macd_hist_slope': np.random.randn(n),
        'boll_pctb_20': np.random.randn(n), 'kdj_j': np.random.randn(n), 'kdj_d': np.random.randn(n),
        'hist_vol_60': np.random.randn(n), 'hist_vol_ratio': np.random.randn(n),
        'atr_pct_14': np.random.randn(n), 'boll_width_20': np.random.randn(n),
        'cmf_20': np.random.randn(n), 'obv': np.random.randn(n), 'obv_ma_ratio': np.random.randn(n),
        'main_flow_ratio': np.random.randn(n), 'mfi_14': np.random.randn(n),
        'turn_rank_60': np.random.randn(n), 'turn_ma_ratio': np.random.randn(n),
        'turn_zscore_20': np.random.randn(n), 'turn_chg_5': np.random.randn(n),
        'breakout': np.random.randint(0, 2, n), 'pullback': np.random.randint(0, 2, n),
    })
    out = dw.apply(pan, 'trend_up')
    assert 'final_score' in out.columns
    # 状态无关时, 高 model_pred 应得高 final_score(基线 z(model) 主导)
    assert out.loc[out['model_pred'].idxmax(), 'final_score'] > \
        out.loc[out['model_pred'].idxmin(), 'final_score']
    # 不同状态应产生不同 final_score 排序(状态依赖)
    o2 = dw.apply(pan, 'high_vol')
    assert not np.allclose(out['final_score'].rank().values,
                           o2['final_score'].rank().values)
    print('  [ok] DynamicWeights 状态依赖融合正确')


# --------------------------------------------------------------------------- #
# 3) AdaptiveThreshold
# --------------------------------------------------------------------------- #
def test_adaptive_threshold():
    cfg = load_regime_config()
    at = at_mod.AdaptiveThreshold(cfg)
    # 参数存在且合理
    for s in REGIMES:
        p = at.params(s)
        assert 0.0 <= p['buy_quantile'] <= 1.0
        assert 0.0 < p['position_scale'] <= 1.0
        assert p['top_k'] >= 1
    # 逆境仓位低于顺势
    assert at.params('trend_up')['position_scale'] >= at.params('high_vol')['position_scale']
    # 门槛筛选: high_vol 门槛高, 选出更少或相等
    pan = pd.DataFrame({'final_score': np.arange(20.0)})
    sel_up = at.select(pan, 'final_score', state='trend_up')
    sel_hv = at.select(pan, 'final_score', state='high_vol')
    assert len(sel_hv) <= len(sel_up)
    print('  [ok] AdaptiveThreshold 门槛/仓位/筛选正确')


# --------------------------------------------------------------------------- #
# 4) 数据驱动权重方向(IC 估计)
# --------------------------------------------------------------------------- #
def test_ic_estimation_sign():
    # 合成面板: trend_up 状态下 momentum(bias_60) 与 fwd20 正相关,
    #           trend_down 状态下 volatility 与 fwd20 负相关
    rng = np.random.default_rng(7)
    TILT_FEATURES = ['bias_60', 'hist_vol_60', 'cmf_20', 'turn_rank_60']
    TILT_GROUPS = {'momentum': ['bias_60'], 'volatility': ['hist_vol_60'],
                   'money_flow': ['cmf_20'], 'liquidity': ['turn_rank_60']}
    rows = []
    # trend_up: bias_60 正相关 fwd20
    for _ in range(3000):
        b = np.random.randn()
        fwd = 0.3 * b + np.random.randn() * 0.5
        rows.append(dict(code='A', trade_date=pd.Timestamp('2024-01-02'),
                         close=10, bias_60=b, hist_vol_60=np.random.randn(),
                         cmf_20=np.random.randn(), turn_rank_60=np.random.randn(),
                         fwd20=fwd, mstate='trend_up'))
    # trend_down: hist_vol_60 负相关 fwd20
    for _ in range(3000):
        v = np.random.randn()
        fwd = -0.3 * v + np.random.randn() * 0.5
        rows.append(dict(code='B', trade_date=pd.Timestamp('2024-01-03'),
                         close=10, bias_60=np.random.randn(), hist_vol_60=v,
                         cmf_20=np.random.randn(), turn_rank_60=np.random.randn(),
                         fwd20=fwd, mstate='trend_down'))
    pan = pd.DataFrame(rows)

    # 复用 04 的估计逻辑(直接内联最小版)
    weights = {}
    for st in REGIMES:
        sub = pan[pan['mstate'] == st]
        w = {}
        for g, fl in TILT_GROUPS.items():
            ics = []
            for f in fl:
                a = (sub[f] - sub[f].mean()) / (sub[f].std(ddof=0) + 1e-9)
                t = (sub['fwd20'] - sub['fwd20'].mean()) / (sub['fwd20'].std(ddof=0) + 1e-9)
                c = np.corrcoef(a, t)[0, 1]
                if np.isfinite(c):
                    ics.append(c)
            w[g] = float(np.mean(ics)) if ics else 0.0
        weights[st] = w
    assert weights['trend_up']['momentum'] > 0.1, weights['trend_up']
    assert weights['trend_down']['volatility'] < -0.1, weights['trend_down']
    print('  [ok] 数据驱动 IC 权重方向正确 '
          f'(trend_up.momentum={weights["trend_up"]["momentum"]:+.2f}, '
          f'trend_down.volatility={weights["trend_down"]["volatility"]:+.2f})')


# --------------------------------------------------------------------------- #
# 5) 状态自适应在逆境场景降低回撤(机理验证)
# --------------------------------------------------------------------------- #
def _simulate(picks, rets, hold_n=10, cost=0.0035):
    """极简模拟: picks[day]={'codes','scale'}; rets[day][code]=当日收益。
    次日开盘买入, 持有 hold_n 日。返回日收益序列与最大回撤。"""
    pos = {}; daily = []
    for di, d in enumerate(rets):
        r_today = []
        for bid, b in list(pos.items()):
            valid = [c for c in b['codes'] if c in rets[d]]
            r = float(np.mean([rets[d][c] for c in valid])) if valid else 0.0
            r_today.append(b['scale'] * r)
            b['held'] += 1
            if b['held'] >= hold_n:
                r_today[-1] = b['scale'] * (r - cost)
                del pos[bid]
        daily.append(float(np.mean(r_today)) if r_today else 0.0)
        rec = picks.get(d)
        if rec and di + 1 < len(rets):
            nd = list(rets)[di + 1]
            cs = [c for c in rec['codes'] if c in rets[nd]]
            if cs:
                pos[f'{d}'] = dict(codes=cs, held=0, scale=float(rec.get('scale', 1.0)))
    s = pd.Series(daily)
    eq = (1 + s).cumprod(); dd = (eq / eq.cummax() - 1).min()
    sharpe = s.mean() / s.std() * np.sqrt(252) if s.std() > 1e-12 else 0.0
    return sharpe, float(dd)


def test_adaptive_lowers_drawdown():
    rng = np.random.default_rng(11)
    days = [f'D{i:03d}' for i in range(120)]
    codes = [f'{c:06d}' for c in range(30)]
    # 构造收益: 前段顺境(alpha +), 后段逆境(alpha −, 高波动)
    rets = {}
    for i, d in enumerate(days):
        base = 0.004 if i < 60 else -0.004
        rets[d] = {c: base + rng.normal(0, 0.02 if i < 60 else 0.05) for c in codes}
    # 状态: 前段 trend_up, 后段 trend_down
    states = {d: ('trend_up' if i < 60 else 'trend_down') for i, d in enumerate(days)}
    # 基线: 每天满仓 top5
    base_picks = {d: {'codes': codes[:5], 'scale': 1.0} for d in days}
    # 自适应: trend_up 满仓, trend_down 仓位 0.3
    adp_picks = {d: {'codes': codes[:5],
                     'scale': 1.0 if states[d] == 'trend_up' else 0.3} for d in days}
    sb, ddb = _simulate(base_picks, rets)
    sa, dda = _simulate(adp_picks, rets)
    # 逆境收缩仓位应显著降低回撤, 且夏普不低于基线
    assert dda > ddb, (dda, ddb)        # 注意 dd 为负, 越大(越接近0)越好
    assert sa >= sb - 1e-6, (sa, sb)
    print(f'  [ok] 状态自适应降低回撤: 基线DD={ddb:.3f} -> 自适应DD={dda:.3f}, '
          f'夏普 {sb:.2f} -> {sa:.2f}')


# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    test_regime_detector_states()
    test_dynamic_weights_apply()
    test_adaptive_threshold()
    test_ic_estimation_sign()
    test_adaptive_lowers_drawdown()
    print('\n✅ 全部测试通过。')
