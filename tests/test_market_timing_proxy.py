# -*- coding: utf-8 -*-
"""测试择时闸门数据源修复(2026-11-16)。

核心: kline_cache[000001] 是平安银行个股错乱数据(收盘7~13元), 曾被误当上证指数
导致熊市恒判"强势"。修复后 market_timing/risk_gate 改用 tools.index_data 实时抓取
真·沪深300(000300), 并做强量级校验(>500点)与新鲜度校验; 数据失效时保守降级为
"弱势/观望", 而非静默"强势满仓"。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _import_tools_mod():
    from tools import index_data, market_timing, risk_gate
    return index_data, market_timing, risk_gate


index_data, market_timing, risk_gate = _import_tools_mod()


def test_index_data_rejects_wrong_level_stock_data(monkeypatch):
    """量级校验: 点位数<500 的"个股错乱数据"必须被拒绝(平安银行 7~13 元)。"""
    # 构造模拟错乱数据: 收盘 7~13 元(平安银行), 而非指数 3600+
    fake_rows = [(f'2026-10-{i:02d}', 10.0, 10.5, 11.0, 9.5) for i in range(1, 31)]
    monkeypatch.setattr(index_data, 'fetch_index', lambda code, count=120: fake_rows)
    # 清空缓存避免命中
    cache_fp = index_data.CACHE_DIR / '000300.json'
    monkeypatch.setattr(index_data, 'CACHE_DIR', ROOT / 'data' / '_test_cache_tmp')
    dates, closes, meta = index_data.load_index_closes('000300', n=20)
    assert closes == [] or meta.get('warn'), "错乱个股数据应被拒绝"


def test_market_verdict_conservative_on_data_failure(monkeypatch):
    """数据失效时 market_verdict 应返回'弱势'而非'强势'。"""
    def fake(n=66):
        return [], [], {'source': 'none', 'warn': '模拟接口失败'}
    monkeypatch.setattr(market_timing, '_proxy_closes', fake)
    v = market_timing.market_verdict()
    assert v['label'] == '弱势', f"数据失效应保守判弱势, 实际 {v['label']}"
    assert v['position_scale'] <= 0.3, "弱势仓位应收缩"


def test_market_verdict_conservative_on_stale_data(monkeypatch):
    """数据陈旧(最新交易日距今过久)时保守判'弱势'。"""
    def fake(n=66):
        dates = [f'2026-09-{i:02d}' for i in range(1, 61)]
        closes = [4000.0 - i * 5 for i in range(60)]
        return dates, closes, {'fresh': False, 'warn': '数据陈旧'}
    monkeypatch.setattr(market_timing, '_proxy_closes', fake)
    v = market_timing.market_verdict()
    assert v['label'] == '弱势', f"数据陈旧应保守判弱势, 实际 {v['label']}"


def test_market_verdict_bear_when_below_ma(monkeypatch):
    """真实下跌市: 收盘<MA60 应判'弱势'。"""
    def fake(n=66):
        # 构造一路下跌: 从 5000 跌到 3500
        dates = [f'2026-{10+i//28:02d}-{i%28+1:02d}' for i in range(66)]
        closes = [5000.0 - i * 23 for i in range(66)]  # 最后 ~3500
        return dates, closes, {'fresh': True, 'warn': ''}
    monkeypatch.setattr(market_timing, '_proxy_closes', fake)
    v = market_timing.market_verdict()
    assert v['label'] == '弱势', f"下跌市应判弱势, 实际 {v['label']}"


def test_market_verdict_bull_when_above_ma(monkeypatch):
    """真实上涨市: 收盘>MA20 应判'强势'。"""
    def fake(n=66):
        dates = [f'2026-{10+i//28:02d}-{i%28+1:02d}' for i in range(66)]
        closes = [3500.0 + i * 23 for i in range(66)]  # 最后 ~5000
        return dates, closes, {'fresh': True, 'warn': ''}
    monkeypatch.setattr(market_timing, '_proxy_closes', fake)
    v = market_timing.market_verdict()
    assert v['label'] == '强势', f"上涨市应判强势, 实际 {v['label']}"


def test_risk_gate_conservative_on_data_failure(monkeypatch):
    """risk_gate 数据失效时应保守暂停(True), 而非放行。"""
    def fake(n):
        return [], [], {'warn': '模拟接口失败'}
    monkeypatch.setattr(risk_gate, '_proxy_rows', fake)
    halt, reason = risk_gate.crash_guard()
    assert halt is True, "数据失效应保守暂停开仓"
    assert '保守' in reason or '失效' in reason


def test_risk_gate_hard_crash_detection(monkeypatch):
    """真实暴跌: 偏离MA60<-15% 应触发硬熔断。"""
    def fake(n):
        # 前60天平稳 4000, 最后暴跌到 3000(-25%)
        dates = [f'2026-{10+i//28:02d}-{i%28+1:02d}' for i in range(62)]
        closes = [4000.0] * 61 + [3000.0]
        return dates, closes, {'fresh': True, 'warn': ''}
    monkeypatch.setattr(risk_gate, '_proxy_rows', fake)
    halt, reason = risk_gate.crash_guard()
    assert halt is True, "暴跌应触发硬熔断"
    assert 'MA60' in reason


def test_index_data_uses_correct_proxy_default():
    """market_timing 默认 proxy 应为沪深300(000300), 不再是错乱的 000001。"""
    assert market_timing.INDEX_CODE == '000300', "默认 proxy 应改为沪深300"
    assert risk_gate.INDEX_CODE == '000300', "risk_gate 默认 proxy 应改为沪深300"
