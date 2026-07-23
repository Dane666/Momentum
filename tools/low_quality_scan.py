# -*- coding: utf-8 -*-
"""
低位绩优股尾盘扫描 (14:45 与动量策略并行)
=========================================
思路: 复用 opt_study/harness_oversold_quality 的"超跌 + 绩优"判定
      (即已验证的 V2 口径), 在每日 14:45 收盘附近筛选低位绩优股:

  低位 : 深度超跌 —— 距 60 日高回撤 ≤ -15% 且 收盘跌破 60 日线(乖离≥3%) 且 RSI<35
  绩优 : ROE≥8% & 净利润同比>0 & PE≤50 & PB≤10 (point-in-time 真实基本面)
  热门 : 过去 30 日资金净流入 Top-K 题材成分股(加分项, 非硬约束)

输出: 低位绩优候选 → data/picks_tracking.json(type=LOW_QUALITY) → Bark
依赖: qlib_pro_v16.db (kline_cache + fundamentals + stock_sector_cache)
      BARK_DEVICE_KEY 环境变量(由 workflow / 本地 shell 注入)
"""
import json, logging, os, sys, sqlite3
from datetime import datetime
from pathlib import Path

_PROJ = Path(__file__).resolve().parent.parent
_sys_parent = str(_PROJ.parent)
if _sys_parent not in sys.path:
    sys.path.insert(0, _sys_parent)
try:
    import momentum as _m  # noqa: F401
except ImportError:
    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        'momentum', _PROJ / '__init__.py',
        submodule_search_locations=[str(_PROJ)])
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules['momentum'] = _mod
    _spec.loader.exec_module(_mod)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('low_quality_scan')

DB_PATH = os.environ.get('MOMENTUM_DB_PATH',
                         os.path.join(str(_PROJ), 'qlib_pro_v16.db'))

# 低位绩优 筛选口径 (对齐已验证 V2, dd 略放宽到 -15% 以保证日更候选量)
SCAN_CFG = dict(mode="deep", dd=-0.15, gap=0.03, rsi_th=35,
                ma60_rising=False, vol_confirm=False, macd_rsi=False,
                hot_on=True, pe_pb_on=True, quality_on=True)
TOP_N = 15
SL_RATIO = 0.85   # 止损 -15% (对齐发布组合)
TP_RATIO = 1.15   # 反弹目标 +15%


# --------------------------------------------------------------------------- #
# 引导 harness 模块(只导入, 不改写); 仅在导入时解耦绝对路径
# --------------------------------------------------------------------------- #
def _load_harness():
    p = _PROJ / 'opt_study' / 'harness_oversold_quality.py'
    import importlib.util
    spec = importlib.util.spec_from_file_location('harness_oversold_quality', p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    # 解耦绝对路径: 全部走当前仓库的 DB / 全量窗口
    m.DB = DB_PATH
    m.ROOT = str(_PROJ)
    m.WINDOW_START = "2024-01-01"
    m.WINDOW_END = "2099-12-31"
    return m


def ensure_names():
    """全 A 代码→名称映射: 优先本地缓存, 否则用 adata 拉取并落盘(需网络, CI 可用)。"""
    cache_file = _PROJ / 'data' / 'stock_names.json'
    names = {}
    if cache_file.exists():
        try:
            names = json.loads(cache_file.read_text(encoding='utf-8'))
        except Exception:
            names = {}
    if names:
        return names
    try:
        import adata.stock.info as stock_info
        df = stock_info.all_code()
        if df is not None and not df.empty:
            code_col = 'stock_code' if 'stock_code' in df.columns else 'code'
            name_col = 'short_name' if 'short_name' in df.columns else 'name'
            for _, r in df.iterrows():
                c = str(r.get(code_col, '') or '').strip()
                n = str(r.get(name_col, '') or '').strip()
                if c and n:
                    names[c] = n
            cache_file.write_text(json.dumps(names, ensure_ascii=False),
                                  encoding='utf-8')
            logger.info(f"[低位绩优] 名称缓存已更新: {len(names)} 只")
    except Exception as e:
        logger.warning(f"[低位绩优] 名称拉取失败(回退代码): {e}")
    return names


def _load_names():
    """股票名称: 1) 全 A 映射(adata 缓存) 2) factor_logs 最新覆盖。"""
    names = ensure_names()
    # factor_logs 最新名称(覆盖新近上市 / 更名)
    try:
        con = sqlite3.connect(DB_PATH)
        for code, name in con.execute(
                "SELECT code, name FROM factor_logs WHERE name IS NOT NULL"):
            names.setdefault(code, name)
        con.close()
    except Exception:
        pass
    return names


# --------------------------------------------------------------------------- #
# 预取: 刷新 kline_cache 中已有股票的当日 bar(对齐 momentum-scan 两阶段)
# --------------------------------------------------------------------------- #
def warm_cache(limit=None):
    try:
        import importlib.util
        from momentum import config as cfg  # noqa: F401
        from momentum.data import (load_or_fetch_kline, fetch_kline_from_api,
                                   init_db, fetch_all_stock_codes)
        init_db()
        # 可分析宇宙 = kline_cache 中已有历史且为主板/非 ST 的股票
        con = sqlite3.connect(DB_PATH)
        cached = [r[0] for r in con.execute(
            "SELECT DISTINCT code FROM kline_cache")]
        con.close()
        codes = [c for c in cached
                 if c.startswith(('60', '00')) and 'ST' not in c]
        if limit:
            codes = codes[:limit]
        logger.info(f"[低位绩优] 预取 K线 {len(codes)} 只 (刷新当日 bar)")
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time
        start = time.time()
        ok = fail = 0
        with ThreadPoolExecutor(max_workers=20) as pool:
            futs = {pool.submit(load_or_fetch_kline, c,
                                fetch_kline_from_api, "2024-01-01"): c
                    for c in codes}
            for f in as_completed(futs):
                try:
                    df = f.result(timeout=30)
                    if df is not None and not df.empty:
                        ok += 1
                    else:
                        fail += 1
                except Exception:
                    fail += 1
        logger.info(f"[低位绩优] 预取完成: {ok} ok / {fail} fail "
                    f"in {time.time()-start:.0f}s")
    except Exception as e:
        logger.warning(f"[低位绩优] 预取跳过(将仅用缓存数据): {e}")


# --------------------------------------------------------------------------- #
# Bark 推送(对齐 c_tail_scan / workflow 内联实现)
# --------------------------------------------------------------------------- #
def bark_push(title: str, body: str):
    try:
        import requests
        key = os.environ.get('BARK_DEVICE_KEY', '').strip()
        if not key:
            logger.warning("[低位绩优] BARK_DEVICE_KEY 未配置, 跳过推送")
            return
        if key.startswith('http'):
            parts = key.rstrip('/').split('/')
            key = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else key)
        requests.post('https://api.day.app/push',
                      json={'device_key': key, 'title': title,
                            'body': body[:3800], 'group': 'Momentum'},
                      timeout=10)
    except Exception as e:
        logger.error(f"[低位绩优] 推送失败: {e}")


# --------------------------------------------------------------------------- #
# tracking / DB 同步 —— 统一走 tools/tracking_utils.add_picks (公共方法)
# --------------------------------------------------------------------------- #
def _is_garbage(name, close):
    """剔除风险警示/退市/仙股: ST、*ST、退、警示 字样, 或股价 < 1.5 的仙股。
    避免绩优策略误推 ST/*ST(如 *ST美丽) 或垃圾低价股。"""
    s = str(name)
    if 'ST' in s or '退' in s or s.startswith('*') or '警示' in s:
        return True
    if close is not None and close < 1.5:
        return True
    return False


# --------------------------------------------------------------------------- #
# 扫描主逻辑
# --------------------------------------------------------------------------- #
def run(scan_date=None, top_n=TOP_N, prefetch=True):
    logger.info("=" * 50)
    logger.info("[低位绩优] 尾盘扫描启动")
    H = _load_harness()
    if prefetch:
        warm_cache()

    import pandas as pd
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    # 绩优数据(基本面): 缺失则优雅降级为"仅低位"筛选, 不崩溃
    fmap = {}
    quality_available = False
    try:
        fmap = H.load_fundamentals()
        if fmap:
            quality_available = True
    except Exception as e:
        logger.warning(f"[低位绩优] fundamentals 读取失败, 退化为仅低位筛选: {e}")
    if not quality_available:
        logger.warning("[低位绩优] 基本面数据缺失: 仅按低位(深度超跌)筛选, 绩优未验证")
    names = _load_names()

    cal = sorted({t for g in ctx.values() for t in g.index})
    if not cal:
        logger.error("[低位绩优] 无 K线数据, 终止")
        return [], "无 K线数据"
    if scan_date:
        target = pd.Timestamp(scan_date)
        cands = [t for t in cal if t <= target]
        last = cands[-1] if cands else cal[-1]
    else:
        last = cal[-1]
    today = str(last)[:10]
    logger.info(f"[低位绩优] 扫描日={today} 标的数={len(ctx)}")

    # 热门题材(加分项)
    hot_at = H.build_hot_themes(ctx, [last])
    hot_codes = hot_at.get(today, (set(), [], {}))[0]

    cfg = dict(SCAN_CFG)
    picks = []
    for code, g in ctx.items():
        if last not in g.index:
            continue
        if g.index.get_loc(last) < 60:
            continue
        # 低位(深度超跌技术形态)
        if not H.base_signal(g, last, cfg):
            continue
        close = float(g.loc[last, 'close'])
        r = g.loc[last]
        dd60 = r['dd60']
        # 250 日高回撤(低位丰富度), 不足 250 日则回退 dd60
        hi250 = g['close'].rolling(250, min_periods=120).max().iloc[-1]
        dd250 = (close / hi250 - 1.0) if (hi250 and hi250 > 0) else dd60
        is_hot = code in hot_codes
        name = names.get(code, code)
        if _is_garbage(name, close):
            continue

        if quality_available:
            # 绩优(真实基本面 point-in-time)
            ok, pe, pb, roe, np_yoy = H.quality_ok(fmap, code, today, close, True)
            if not ok:
                continue
            # 评分: 深度*0.5 + ROE*0.3 + 净利同比(封顶100)*0.1 + 热门+10
            score = (-dd60 * 100) * 0.5 + (roe or 0) * 0.3 \
                + min(np_yoy or 0, 100) * 0.1 + (10 if is_hot else 0)
            picks.append(dict(
                code=code, name=name, price=round(close, 2),
                dd60=round(float(dd60) * 100, 1),
                dd250=round(float(dd250) * 100, 1),
                roe=roe, np_yoy=np_yoy,
                pe=round(float(pe), 1) if pe else None,
                pb=round(float(pb), 1) if pb else None,
                hot=is_hot, score=round(score, 2), quality_unverified=False))
        else:
            # 基本面缺失: 仅按低位(深度超跌)筛选, 绩优未验证(保证 CI 不崩溃且有候选)
            # 垃圾过滤已由 _is_garbage(name, close) 统一处理
            score = (-dd60 * 100) * 0.5 + (10 if is_hot else 0)
            picks.append(dict(
                code=code, name=name, price=round(close, 2),
                dd60=round(float(dd60) * 100, 1),
                dd250=round(float(dd250) * 100, 1),
                roe=None, np_yoy=None, pe=None, pb=None,
                hot=is_hot, score=round(score, 2), quality_unverified=True))

    picks.sort(key=lambda x: -x['score'])
    picks = picks[:top_n]
    report = _build_report(picks, today)
    print(report)
    if picks and os.environ.get('LOW_QUALITY_NO_TRACK') != '1':
        from momentum.tools.tracking_utils import add_picks
        add_picks(picks, 'LOW_QUALITY', SL_RATIO, TP_RATIO, date=today)
    title = f"📉 低位绩优股 {today}" if picks else f"📉 低位绩优股 {today}(空)"
    bark_push(title, report)
    logger.info("[低位绩优] 完成")
    return picks, report


def _build_report(picks, today):
    unverified = bool(picks) and bool(picks[0].get('quality_unverified'))
    if unverified:
        sub = "低位=深度超跌(距60日高≤-15%&RSI<35) [基本面缺失: 仅按低位筛选, 绩优未验证]"
        score_desc = "评分=超跌深度×0.5+热门+10 (低位+热门)"
    else:
        sub = "低位=深度超跌(距60日高≤-15%&RSI<35) + 绩优(ROE≥8%&净利>0&PE≤50&PB≤10)"
        score_desc = "评分=超跌深度×0.5+ROE×0.3+净利×0.1+热门+10"
    lines = [f"📉 低位绩优股筛选 | {today}", sub,
             f"命中 {len(picks)} 只 (按 超跌深度/ROE/净利/热门 评分)"]
    if not picks:
        lines.append("（今日无满足双条件的标的）")
        return "\n".join(lines)
    lines.append("─" * 40)
    for i, p in enumerate(picks, 1):
        hot = " 🔥" if p['hot'] else ""
        pe_s = f"PE{p['pe']}" if p.get('pe') else "PE-"
        pb_s = f"PB{p['pb']}" if p.get('pb') else "PB-"
        roe_s = f"ROE{p['roe']:.0f}%" if p.get('roe') is not None else "ROE-"
        np_s = f"净利{p['np_yoy']:.0f}%" if p.get('np_yoy') is not None else "净利-"
        lines.append(
            f"{i:>2}. {p['code']} {p['name']}{hot}\n"
            f"    ¥{p['price']:.2f}  超跌{p['dd60']:.0f}%  250低{p['dd250']:.0f}%  "
            f"{roe_s}  {np_s}  {pe_s} {pb_s}")
    lines.append("─" * 40)
    lines.append(score_desc)
    lines.append(f"止损¥{picks[0]['price']*SL_RATIO:.2f} 反弹目标¥{picks[0]['price']*TP_RATIO:.2f}(参考)")
    return "\n".join(lines)


if __name__ == "__main__":
    sd = os.environ.get('LOW_QUALITY_SCAN_DATE')
    pf = os.environ.get('LOW_QUALITY_PREFETCH', '1') != '0'
    run(scan_date=sd, prefetch=pf)
