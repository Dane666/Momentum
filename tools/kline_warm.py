# -*- coding: utf-8 -*-
"""
全市场 K 线缓存预热 (供实时行情降级兜底)
=========================================
参考低位绩优股模块的健壮性思路: 在 14:25 主动把全市场 K 线预热进
qlib_pro_v16.db 的 kline_cache, 使 data/fetcher.fetch_realtime_quotes 的三级
降级 (新浪→efinance→K线缓存) 在实时接口失败时仍能覆盖全市场, 而非仅 ~200 只.

性能优化 (用户要求): 不每天重抓全历史.
- 快速路径: 一次 bulk 实时行情 (fetch_realtime_quotes 单调用拿全市场) ->
  把当日 bar upsert 进 kline_cache. 历史常驻, 每日只补"当天", 一次调用覆盖 5000 只.
- 兜底路径: 实时失败时, 逐只 load_or_fetch_kline (mootdx/tencent), 其写库本身是
  增量的 (只追加 >= last_cached 的新 bar), 故次日仍只需补当天.

两条路径都幂等, 重复运行安全. 设计为零副作用: 实时成功时动量/C尾扫描根本
不触发降级, 本模块只是"备用弹药".
"""
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

_PROJ = Path(__file__).resolve().parent.parent
_sys_parent = str(_PROJ.parent)
if _sys_parent not in sys.path:
    sys.path.insert(0, _sys_parent)

logger = logging.getLogger('kline_warm')

DB_PATH = os.environ.get('MOMENTUM_DB_PATH',
                         os.path.join(str(_PROJ), 'qlib_pro_v16.db'))


def _real_bulk_to_bars(df_real) -> "object":
    """把 bulk 实时行情 DataFrame 转成 kline_cache 的日 bar 格式.

    仅取非 ST/退 的 A 股, 用 最新价/最高/最低/今开/成交量/成交额 构造当日 bar.
    返回 pandas.DataFrame 或 None.
    """
    import pandas as pd
    df = df_real.copy()
    df['code'] = df['股票代码'].astype(str)
    keep = df[~df['code'].str.contains('ST|退', na=False)]
    rows = []
    today = datetime.now().strftime('%Y-%m-%d')
    for _, r in keep.iterrows():
        try:
            close = pd.to_numeric(r.get('最新价'), errors='coerce')
            high = pd.to_numeric(r.get('最高'), errors='coerce')
            low = pd.to_numeric(r.get('最低'), errors='coerce')
            open_ = pd.to_numeric(r.get('今开'), errors='coerce')
            vol = pd.to_numeric(r.get('成交量'), errors='coerce')
            amt = pd.to_numeric(r.get('成交额'), errors='coerce')
            if pd.isna(close) or close <= 0:
                continue
            rows.append({
                'code': str(r['code']).zfill(6),
                'trade_date': today,
                'open': float(open_) if pd.notna(open_) else float(close),
                'high': float(high) if pd.notna(high) else float(close),
                'low': float(low) if pd.notna(low) else float(close),
                'close': float(close),
                'volume': float(vol) if pd.notna(vol) else 0.0,
                'amount': float(amt) if pd.notna(amt) else 0.0,
                'turnover_ratio': 0.0,
            })
        except Exception:
            continue
    if not rows:
        return None
    return pd.DataFrame(rows)


def warm_universe(limit: int = None, use_realtime_bulk: bool = True) -> dict:
    """预热全市场 K 线缓存.

    Returns:
        dict: {ok, fail, from_bulk, codes}
    """
    try:
        from momentum import config as cfg  # noqa: F401
        from momentum.data import (load_or_fetch_kline, fetch_kline_from_api,
                                   init_db, fetch_all_stock_codes)
        from momentum.data.cache import upsert_kline_bars
        init_db()
    except Exception as e:
        logger.warning(f"[kline_warm] 初始化失败: {e}")
        return dict(ok=0, fail=0, from_bulk=False, codes=0)

    # 全市场非 ST A 股 (60/00/30/68)
    try:
        codes = [c for c in fetch_all_stock_codes()
                 if c.startswith(('60', '00', '30', '68')) and 'ST' not in c]
    except Exception as e:
        logger.warning(f"[kline_warm] 取代码列表失败: {e}")
        codes = []
    if limit:
        codes = codes[:limit]
    codes = list(dict.fromkeys(codes))  # 去重保序
    logger.info(f"[kline_warm] 全市场预热目标 {len(codes)} 只")
    stats = dict(ok=0, fail=0, from_bulk=False, codes=len(codes))
    if not codes:
        return stats

    # ── 快速路径: 一次 bulk 实时行情 -> upsert 当日 bar ──
    if use_realtime_bulk:
        try:
            from momentum.data import fetch_realtime_quotes
            df_real = fetch_realtime_quotes(fs='沪深A股')
            # 仅当是"真·实时"(无 _from_cache 列)才 upsert; 否则是缓存回声, 跳过
            if (df_real is not None and not df_real.empty
                    and '_from_cache' not in df_real.columns):
                bars = _real_bulk_to_bars(df_real)
                if bars is not None and len(bars) > 100:
                    n = upsert_kline_bars(bars)
                    if n > 0:
                        stats['ok'] = n
                        stats['from_bulk'] = True
                        logger.info(f"[kline_warm] bulk 实时预热成功: {n} 只当日 bar 入缓存")
                        return stats
        except Exception as e:
            logger.warning(f"[kline_warm] bulk 实时预热失败, 退回逐只: {e}")

    # ── 兜底路径: 逐只 load_or_fetch_kline (mootdx/tencent), 增量写库 ──
    from concurrent.futures import ThreadPoolExecutor, as_completed
    start = time.time()
    ok = fail = 0
    with ThreadPoolExecutor(max_workers=20) as pool:
        futs = {pool.submit(load_or_fetch_kline, c, fetch_kline_from_api,
                            "2024-01-01"): c for c in codes}
        for f in as_completed(futs):
            try:
                df = f.result(timeout=30)
                if df is not None and not df.empty:
                    ok += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
    stats['ok'] = ok
    stats['fail'] = fail
    logger.info(f"[kline_warm] 逐只预热完成: {ok} ok / {fail} fail in {time.time()-start:.0f}s")
    return stats


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    print(warm_universe())
