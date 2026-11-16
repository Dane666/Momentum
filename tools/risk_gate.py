# -*- coding: utf-8 -*-
"""尾部风险门禁 — 生产链路极端风险硬熔断(crash_only 落地版)。

复刻 opt_study/macro_overwrite.py 的 crash_only 语义, 但独立于研究脚本, 直接消费
kline_cache 里的宽基指数(上证000001)作为大盘 proxy, 不依赖任何策略内部。

仅做"尾部风险保护": 系统性暴跌时暂停新开仓, 不改变选股逻辑、不强制降仓
(已有持仓由 position_monitor 独立监控止损)。回应总结建议②"极端风险过滤"。

触发条件(任一即 halt 暂停开仓):
  1. 硬熔断: 大盘 proxy nav / MA60 - 1 < HARD_THRESHOLD(默认 -0.15)
  2. 单日暴跌: 大盘 proxy 当日收盘较前一交易日跌幅 > DROP_PCT(默认 0.03)
平时返回 (False, '') 不影响正常选股。

用法(建议放在各策略 scan 的 run() 入口):
    from tools.risk_gate import crash_guard
    halt, reason = crash_guard()
    if halt:
        bark_notify(f"⛔ 风险门禁: {reason} 今日暂停选股")
        return []
"""
import sqlite3
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent
DB = str(PROJ / 'qlib_pro_v16.db')
# 大盘 proxy 改用沪深300(000300): kline_cache[000001] 实为平安银行个股错乱数据
# (收盘 7~13 元), 长期被误当上证指数(2026-11-16 定位)。现用 tools.index_data 实时抓取。
INDEX_CODE = '000300'     # 沪深300 作为大盘 proxy(真宽基指数)
MA = 60
HARD_THRESHOLD = -0.15   # nav/MA60 - 1 低于此值 → 硬熔断(与 macro_overwrite 一致)
DROP_PCT = 0.03          # 单日跌幅超此值 → 暂停开仓(总结建议"大盘跌幅>3%暂停")


def _import_index_data():
    """加载 tools.index_data, 兼容 CI(momentum.tools) / 本地直接跑(tools) 两种包路径。"""
    import sys
    if str(PROJ) not in sys.path:
        sys.path.insert(0, str(PROJ))
    try:
        from momentum.tools.index_data import load_index_closes
    except (ImportError, ModuleNotFoundError):
        from tools.index_data import load_index_closes
    return load_index_closes


def _proxy_rows(n):
    """返回 (dates, closes, meta) — 真·宽基指数收盘序列(升序, 最近 n 条)。"""
    try:
        load_index_closes = _import_index_data()
        dates, closes, meta = load_index_closes(INDEX_CODE, n=n)
        return dates, closes, meta
    except Exception as e:
        return [], [], {'warn': f'index_data 异常: {e}'}


def crash_guard(threshold=HARD_THRESHOLD, drop_pct=DROP_PCT, ma=MA,
                index=INDEX_CODE, db=DB):
    """返回 (halt: bool, reason: str)。数据失效时保守暂停(不误判为安全)。"""
    try:
        dates, closes, meta = _proxy_rows(ma + 2)
        if len(closes) < ma + 1:
            return (True, f'⚠️ 大盘proxy数据不足/失效({meta.get("warn") or "无有效收盘"}), 保守暂停开仓')
        if meta.get('warn') and not meta.get('fresh', True):
            return (True, f'⚠️ 大盘proxy数据异常({meta.get("warn")}), 保守暂停开仓')
        nav, prev = closes[-1], closes[-2]
        ma60 = sum(closes[-ma:]) / ma
        dev = nav / ma60 - 1.0
        if dev < threshold:
            return (True, f"大盘proxy跌破MA60{threshold*100:.0f}% (偏离{dev*100:.1f}%, {dates[-1]})")
        day_ret = nav / prev - 1.0
        if day_ret < -drop_pct:
            return (True, f"大盘proxy单日暴跌{day_ret*100:.1f}% (>{(-drop_pct)*100:.0f}%暂停)")
        return (False, '')
    except Exception as e:
        return (True, f'⚠️ 门禁检查异常(保守暂停): {e}')


if __name__ == '__main__':
    h, r = crash_guard()
    print(f"halt={h} reason={r!r}")
