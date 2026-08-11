# -*- coding: utf-8 -*-
"""市场择时闸门 — 回答"模型 Top10 今天是否值得买"。

模型是横截面排序 alpha(强势延续), 但绝对收益受大盘环境驱动: 熊市里
"最好的那批"往往也只是"跌得少的那批"。本模块给出当日开仓 verdict,
供 Bark 推送与(可选)自动降仓使用。

三档(基于大盘 proxy 上证000001 与 MA20/MA60 的位置):
  - bull   强势: proxy ≥ MA20        -> 全买 Top10
  - ranging 中性: MA60 ≤ proxy < MA20 -> 半仓(优选前5)
  - bear   弱势: proxy <  MA60        -> 观望(仅前3 / 或空仓)

另复用 tools.risk_gate.crash_guard 做尾部硬熔断(暴跌/破MA60-15% 暂停开仓)。
任何异常都放行(不阻断选股), 与 risk_gate 一致。
"""
import sqlite3
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent
DB = str(PROJ / 'qlib_pro_v16.db')
INDEX_CODE = '000001'          # 上证指数作为大盘 proxy(与 risk_gate / config 一致)
MA20 = 20
MA60 = 60


def _load_regime_scale_map():
    """从 config/regime_config.yaml 的 adaptive 段读取 position_scale, 映射到
    market_timing 三档 label(强势/中性/弱势/熔断/未知)。失败回退默认常量。"""
    default = {'强势': 1.00, '中性': 0.65, '弱势': 0.30, '熔断': 0.0, '未知': 0.80}
    try:
        import yaml
        cfg_path = PROJ / 'config' / 'regime_config.yaml'
        if cfg_path.exists():
            cfg = yaml.safe_load(open(cfg_path, encoding='utf-8'))
            ad = (cfg or {}).get('adaptive', {})
            sc = {s: float(d.get('position_scale', 1.0)) for s, d in ad.items()}
            return {
                '强势': sc.get('trend_up', default['强势']),
                '中性': sc.get('range', default['中性']),
                '弱势': min(sc.get('trend_down', default['弱势']),
                            sc.get('high_vol', default['弱势'])),
                '熔断': 0.0,
                '未知': default['未知'],
            }
    except Exception:
        pass
    return default


_SCALE_MAP = _load_regime_scale_map()


def position_scale_for(label: str) -> float:
    """返回某市场状态 label 对应的建议仓位比例(0~1)。供策略闸门与 Bark 推送使用。"""
    return float(_SCALE_MAP.get(label, 0.80))


def _proxy_closes(n=66):
    """返回 (dates, closes) 最近 n 个交易日的 proxy 收盘序列。"""
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT trade_date, close FROM kline_cache WHERE code=? "
        "ORDER BY trade_date DESC LIMIT ?",
        (INDEX_CODE, n)).fetchall()
    con.close()
    rows.reverse()
    return [d for d, _ in rows], [float(c) for _, c in rows]


def market_verdict():
    """返回当日择时 verdict dict: state/label/action/msg/dev20/dev60/nav。"""
    try:
        dates, closes = _proxy_closes(MA60 + 6)
        if len(closes) < MA20 + 1:
            return dict(state='unknown', crash=False, label='未知',
                        action='全买 Top10',
                        msg='大盘数据不足, 按常态推荐全买')
        nav = closes[-1]
        ma20 = sum(closes[-MA20:]) / MA20
        ma60 = sum(closes[-MA60:]) / MA60
        dev20 = nav / ma20 - 1.0
        dev60 = nav / ma60 - 1.0
        if nav >= ma20:
            state, label, action = 'bull', '强势', '全买 Top10'
        elif nav >= ma60:
            state, label, action = 'ranging', '中性', '半仓(优选前5)'
        else:
            state, label, action = 'bear', '弱势', '观望(仅前3 / 空仓)'
        msg = (f"大盘proxy偏离MA20 {dev20*100:+.1f}% | 偏离MA60 {dev60*100:+.1f}%")
        return dict(state=state, crash=False, label=label, action=action, msg=msg,
                    dev20=round(dev20, 4), dev60=round(dev60, 4), nav=round(nav, 2),
                    position_scale=position_scale_for(label))
    except Exception as e:
        return dict(state='unknown', crash=False, label='未知', action='全买 Top10',
                    msg=f'择时检查异常(放行): {e}',
                    position_scale=position_scale_for('未知'))


def timing_gate():
    """综合 verdict: 先查暴跌硬熔断, 再给三档状态。

    返回 dict:
      halt(bool) 今日是否暂停开仓(仅暴跌硬熔断会 True)
      reason(str) 暂停原因(否则 '')
      verdict(dict) market_verdict() 的结果
    """
    try:
        from momentum.tools.risk_gate import crash_guard
        halt, reason = crash_guard()
    except Exception:
        halt, reason = False, ''
    verdict = market_verdict()
    verdict['crash'] = halt
    if halt:
        verdict['action'] = '暂停开仓(暴跌熔断)'
        verdict['label'] = '熔断'
        verdict['position_scale'] = 0.0
    return dict(halt=halt, reason=reason, verdict=verdict)


if __name__ == '__main__':
    import json
    print(json.dumps(timing_gate(), ensure_ascii=False, indent=2))
