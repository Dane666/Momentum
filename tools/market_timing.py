# -*- coding: utf-8 -*-
"""市场择时闸门 — 回答"模型 Top10 候选今天该买几只(≤5)"。

模型是横截面排序 alpha(强势延续), 但绝对收益受大盘环境驱动: 熊市里
"最好的那批"往往也只是"跌得少的那批"。本模块给出当日开仓 verdict,
供 Bark 推送与(可选)自动降仓使用。

三档(基于大盘 proxy 上证000001 与 MA20/MA60 的位置):
  - bull   强势: proxy ≥ MA20        -> 全部买入(≤5)
  - ranging 中性: MA60 ≤ proxy < MA20 -> 半仓(前3)
  - bear   弱势: proxy <  MA60        -> 观望(前2 / 或空仓)

另复用 tools.risk_gate.crash_guard 做尾部硬熔断(暴跌/破MA60-15% 暂停开仓)。
任何异常都放行(不阻断选股), 与 risk_gate 一致。
"""
import sqlite3
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent
DB = str(PROJ / 'qlib_pro_v16.db')
# 大盘 proxy 默认用沪深300(000300): kline_cache 里的 000001 实为"平安银行"个股
# 错乱数据(收盘 7~13 元), 长期被误当上证指数导致择时恒判"强势"(2026-11-16 定位)。
# 现改用 tools.index_data 实时抓取真·宽基指数(新浪/腾讯), 并做强量级+新鲜度校验。
INDEX_CODE = '000300'          # 沪深300 作为大盘 proxy(真宽基指数)
INDEX_NAME = '沪深300'
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
    """返回 (dates, closes, meta) 最近 n 个交易日的 proxy 收盘序列(升序)。

    用 tools.index_data.load_index_closes 实时抓取真·宽基指数(沪深300),
    替代原先读 kline_cache[000001] 的错乱数据(平安银行个股, 非指数)。
    meta 含 source/fresh/warn, 供上层判断数据是否可信。
    """
    try:
        import sys
        if str(PROJ) not in sys.path:
            sys.path.insert(0, str(PROJ))
        try:
            from momentum.tools.index_data import load_index_closes
        except (ImportError, ModuleNotFoundError):
            from tools.index_data import load_index_closes
        dates, closes, meta = load_index_closes(INDEX_CODE, n=n)
        return dates, closes, meta
    except Exception as e:
        return [], [], {'source': 'err', 'warn': f'index_data 加载异常: {e}'}


def market_verdict():
    """返回当日择时 verdict dict: state/label/action/msg/dev20/dev60/nav。

    数据失效(抓取失败/陈旧/量级错乱)时, 不再静默当"强势", 而是保守返回
    '弱势/观望' 并带 ⚠️ 告警 —— 宁可少买, 不可在失真 proxy 下满仓。
    """
    try:
        dates, closes, meta = _proxy_closes(MA60 + 6)
        # 数据不可信 -> 保守降级
        if not closes or len(closes) < MA20 + 1:
            return dict(state='bear', crash=False, label='弱势',
                        action='观望(前2 / 空仓)',
                        msg=f'⚠️ 大盘proxy({INDEX_NAME})数据不足/获取失败'
                            f'({meta.get("warn") or "无有效收盘"}), 按弱势观望处理',
                        position_scale=position_scale_for('弱势'),
                        data_warn=meta.get('warn', ''))
        if not meta.get('fresh', True) or meta.get('warn'):
            return dict(state='bear', crash=False, label='弱势',
                        action='观望(前2 / 空仓)',
                        msg=f'⚠️ 大盘proxy({INDEX_NAME})数据异常'
                            f'({meta.get("warn")}), 按弱势观望处理',
                        position_scale=position_scale_for('弱势'),
                        data_warn=meta.get('warn', ''))
        nav = closes[-1]
        ma20 = sum(closes[-MA20:]) / MA20
        ma60 = sum(closes[-MA60:]) / MA60
        dev20 = nav / ma20 - 1.0
        dev60 = nav / ma60 - 1.0
        if nav >= ma20:
            state, label, action = 'bull', '强势', '全部买入(≤3)'
        elif nav >= ma60:
            state, label, action = 'ranging', '中性', '半仓(前3)'
        else:
            state, label, action = 'bear', '弱势', '观望(前2 / 空仓)'
        msg = (f"大盘proxy({INDEX_NAME})偏离MA20 {dev20*100:+.1f}% | 偏离MA60 {dev60*100:+.1f}%")
        return dict(state=state, crash=False, label=label, action=action, msg=msg,
                    dev20=round(dev20, 4), dev60=round(dev60, 4), nav=round(nav, 2),
                    position_scale=position_scale_for(label))
    except Exception as e:
        return dict(state='bear', crash=False, label='弱势', action='观望(前2 / 空仓)',
                    msg=f'⚠️ 择时检查异常(按弱势观望): {e}',
                    position_scale=position_scale_for('弱势'))


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
