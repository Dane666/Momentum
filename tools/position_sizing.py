# -*- coding: utf-8 -*-
"""仓位管理 — 同时持仓上限 + 风险平价/凯利权重。

设计目的(对应评估报告建议④):
  把"自选 N 只"收敛为"建议同时持有 3~5 只 + 按风险分配权重", 避免单吊 / 过度分散。
  本模块是纯函数库, 被各 scan 在选股后调用, 给每只入选票附上 `weight`(0~1) 与
  `alloc_pct`(百分比), 经 add_picks 落库到 picks_tracking.json, 由 position_monitor
  在提醒中展示, 供人工下单参考(本系统不自动交易)。

两类权重:
  - risk_parity(默认): 权重 ∝ 1/年化波动率(波动低的票配得多, 降低组合波动)
  - kelly: 基于各票历史胜率/赔率算凯利 f(封顶 25%), 缺失则退化为等权
  - equal: 等权(兜底)

同时提供 cap_to_max(): 按 score 取前 max_n 只(持仓上限)。
"""
import os
import sqlite3
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('position_sizing')

PROJ = Path(__file__).resolve().parent.parent
DB = os.environ.get('MOMENTUM_DB_PATH', str(PROJ / 'qlib_pro_v16.db'))

# 同时持仓上限(评估报告建议 3~5 只)。0 = 不限制(保留旧行为, 全量注册)。
MAX_HOLDINGS = int(os.environ.get('MAX_HOLDINGS', '5'))
# 年化波动率估计窗口(交易日)
VOL_WINDOW = int(os.environ.get('POS_VOL_WINDOW', '60'))
# 风险平价兜底最小波动率(避免除零)
MIN_VOL = 0.01
# 凯利单票上限(防止过度集中)
KELLY_CAP = float(os.environ.get('KELLY_CAP', '0.25'))


def _annualized_vol(code, db=DB, window=VOL_WINDOW):
    """取 code 最近 window 日收盘收益率的年化波动率(std * sqrt(252))。失败返回 None。"""
    try:
        con = sqlite3.connect(db)
        rows = con.execute(
            "SELECT close FROM kline_cache WHERE code=? "
            "ORDER BY trade_date DESC LIMIT ?", (str(code), int(window) + 1)).fetchall()
        con.close()
        if len(rows) < 10:
            return None
        closes = [r[0] for r in reversed(rows)]
        rets = [(closes[i] / closes[i - 1] - 1.0) for i in range(1, len(closes))]
        if len(rets) < 5:
            return None
        mean = sum(rets) / len(rets)
        var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
        import math
        sd = math.sqrt(var)
        return max(sd * (252 ** 0.5), MIN_VOL)
    except Exception:
        return None


def kelly_fraction(win_rate, payoff_ratio, cap=KELLY_CAP):
    """单票凯利 f = (p*b - q)/b, 封顶 cap。win_rate∈[0,1], payoff_ratio>0(盈亏比)。"""
    if win_rate is None or payoff_ratio is None or payoff_ratio <= 0:
        return None
    p = max(0.0, min(1.0, float(win_rate)))
    q = 1.0 - p
    b = float(payoff_ratio)
    f = (p * b - q) / b
    if f <= 0:
        return 0.0
    return min(f, cap)


def risk_parity_weights(vols):
    """vols: list[float|None]。权重 = (1/vol) 归一化; 缺失按平均 vol 处理。"""
    n = len(vols)
    if n == 0:
        return []
    avg = sum(v for v in vols if v) / max(1, sum(1 for v in vols if v))
    eff = [v if v else avg for v in vols]
    inv = [1.0 / max(v, MIN_VOL) for v in eff]
    tot = sum(inv) or 1.0
    return [x / tot for x in inv]


def cap_to_max(picks, max_n, score_key='score'):
    """按 score_key 降序取前 max_n 只。无 score 则保持原顺序取前 max_n。"""
    if max_n and max_n > 0 and len(picks) > max_n:
        def _sc(p):
            s = p.get(score_key)
            return s if isinstance(s, (int, float)) else 0.0
        return sorted(picks, key=lambda x: -_sc(x))[:max_n]
    return picks


def build_portfolio(picks, db=DB, max_n=MAX_HOLDINGS, method='risk_parity'):
    """给选股附上建议权重, 返回建议持仓子集(<=max_n 只, 按 score 取前 max_n)。

    Args:
        picks: list[dict], 至少含 'code'; 可选 'score'(排序/优先级),
               'win_rate'/'payoff_ratio'(kelly 用), 'price'。
        max_n: 持仓上限(0=不限制)。
        method: 'risk_parity' | 'kelly' | 'equal'。
    Returns:
        list[dict] 与 picks 顺序一致(已截取前 max_n), 每只包含 'weight'(0~1) 与 'alloc_pct'(%)。
    """
    if not picks:
        return []
    selected = cap_to_max(picks, max_n) if (max_n and max_n > 0) else list(picks)

    vols = [_annualized_vol(p.get('code'), db) for p in selected]
    has_vol = any(v is not None for v in vols)

    if method == 'kelly':
        fs = []
        for p in selected:
            wr = p.get('win_rate')
            pr = p.get('payoff_ratio')
            f = kelly_fraction(wr, pr)
            fs.append(f if f is not None else None)
        if any(f is not None for f in fs):
            # 有凯利数据的用凯利, 其余等权, 再归一化
            base = [ (f if f is not None else 1.0 / len(selected)) for f in fs ]
            tot = sum(base) or 1.0
            weights = [x / tot for x in base]
        else:
            weights = [1.0 / len(selected)] * len(selected)
    elif method == 'equal':
        weights = [1.0 / len(selected)] * len(selected)
    else:  # risk_parity
        if has_vol:
            weights = risk_parity_weights(vols)
        else:
            weights = [1.0 / len(selected)] * len(selected)

    for p, w in zip(selected, weights):
        p['weight'] = round(w, 4)
        p['alloc_pct'] = round(w * 100, 2)
    return selected


def recommend(picks, db=DB, max_n=MAX_HOLDINGS, method='risk_parity'):
    """便捷别名, 供 scan 调用: 返回已附权重的建议持仓子集。"""
    return build_portfolio(picks, db=db, max_n=max_n, method=method)


if __name__ == '__main__':
    import json
    # 演示: 从 picks_tracking.json 读取当日候选并给出建议权重
    tf = PROJ / 'data' / 'picks_tracking.json'
    if not tf.exists():
        print("无 picks_tracking.json, 先跑 scan")
    else:
        recs = json.loads(tf.read_text(encoding='utf-8'))
        cands = [r for r in recs if r.get('status') in ('WATCHING', 'PLAN', 'HOLDING')]
        out = build_portfolio(cands, max_n=MAX_HOLDINGS, method='risk_parity')
        print(f"候选 {len(cands)} 只 → 建议持仓 {len(out)} 只 (max_n={MAX_HOLDINGS}, risk_parity):")
        for p in out:
            print(f"  {p['code']} {p.get('name','')} 权重 {p.get('alloc_pct')}%  "
                  f"vol={_annualized_vol(p['code'])}")
