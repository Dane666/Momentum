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
INDEX_CODE = '000001'     # 上证指数作为大盘 proxy(库内唯一完整宽基指数)
MA = 60
HARD_THRESHOLD = -0.15   # nav/MA60 - 1 低于此值 → 硬熔断(与 macro_overwrite 一致)
DROP_PCT = 0.03          # 单日跌幅超此值 → 暂停开仓(总结建议"大盘跌幅>3%暂停")


def crash_guard(threshold=HARD_THRESHOLD, drop_pct=DROP_PCT, ma=MA,
                index=INDEX_CODE, db=DB):
    """返回 (halt: bool, reason: str)。任何异常都放行(不阻断选股)。"""
    try:
        con = sqlite3.connect(db)
        rows = con.execute(
            "SELECT trade_date, close FROM kline_cache WHERE code=? "
            "ORDER BY trade_date DESC LIMIT ?",
            (index, ma + 2)).fetchall()
        con.close()
        if len(rows) < ma + 1:
            return (False, '')  # 数据不足, 不误触发
        rows.reverse()
        dates = [d for d, _ in rows]
        closes = [c for _, c in rows]
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
        return (False, f'门禁检查异常(放行): {e}')


if __name__ == '__main__':
    h, r = crash_guard()
    print(f"halt={h} reason={r!r}")
