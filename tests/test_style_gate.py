# -*- coding: utf-8 -*-
"""测试弱势日风格闸门 style_gate(2026-11-20)。

实证背景: 8-25~11-13 中小盘深熊期, 银行/红利(防御)+11.5%胜率100%,
题材妖股(进攻)-30%胜率0%。style_gate 负责行业识别 + 防御/进攻风格判定。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.style_gate import (
    classify_style, StyleGate,
    DEFENSIVE_INDUSTRIES, AGGRESSIVE_INDUSTRIES,
    fetch_board_members, fetch_board_index_pct,
)


def test_classify_defensive():
    assert classify_style('银行Ⅱ') == 'defensive'
    assert classify_style('银行') == 'defensive'
    assert classify_style('保险Ⅱ') == 'defensive'
    assert classify_style('电力') == 'defensive'


def test_classify_aggressive():
    assert classify_style('半导体') == 'aggressive'
    assert classify_style('元件') == 'aggressive'
    assert classify_style('通信设备') == 'aggressive'
    assert classify_style('电池') == 'aggressive'


def test_classify_neutral():
    assert classify_style('多元金融') == 'neutral'
    assert classify_style('') == 'unknown'


def test_defensive_and_aggressive_no_overlap():
    overlap = DEFENSIVE_INDUSTRIES & AGGRESSIVE_INDUSTRIES
    assert not overlap, f"防御与进攻行业集合不应有重叠: {overlap}"


def test_filter_defensive_keeps_only_defensive(monkeypatch):
    gate = StyleGate()
    # 构造模拟注释结果, 绕过网络
    fake_ann = {
        '600036': {'name': '招商银行', 'industry': '银行Ⅱ', 'region': '广东', 'style': 'defensive'},
        '600183': {'name': '生益科技', 'industry': '元件', 'region': '广东', 'style': 'aggressive'},
        '002647': {'name': '仁东控股', 'industry': '多元金融', 'region': '广东', 'style': 'neutral'},
    }
    kept = gate.filter_defensive(fake_ann)
    assert kept == ['600036'], f"弱势日应只保留防御票, 实际 {kept}"


def test_sector_resonance_counts_same_industry():
    gate = StyleGate()
    ann = {
        'a': {'industry': '元件', 'style': 'aggressive'},
        'b': {'industry': '元件', 'style': 'aggressive'},
        'c': {'industry': '银行Ⅱ', 'style': 'defensive'},
    }
    res = gate.sector_resonance(ann)
    assert res == {'元件': 2, '银行Ⅱ': 1}


def test_defensive_candidates_filters_and_dedups(monkeypatch):
    """防御候选: 只留涨幅>=0, 去重, 按涨幅降序。mock 掉板块成分接口。"""
    gate = StyleGate()

    def fake_members(bk, limit=80):
        return {
            'BK0475': [
                {'code': '601398', 'name': '工商银行', 'pct': 3.3},
                {'code': '601939', 'name': '建设银行', 'pct': 3.0},
                {'code': '600036', 'name': '招商银行', 'pct': -0.5},  # 下跌应被过滤
            ],
            'BK0437': [
                {'code': '601398', 'name': '工商银行', 'pct': 3.3},  # 重复应去重
                {'code': '601088', 'name': '中国神华', 'pct': 1.5},
            ],
        }.get(bk, [])

    monkeypatch.setattr('tools.style_gate.fetch_board_members', fake_members)
    cands = gate.defensive_candidates(top_n=3, min_pct=0.0)
    codes = [c['code'] for c in cands]
    # 下跌的招商银行被过滤; 重复的工商银行只出现一次
    assert '600036' not in codes
    assert codes.count('601398') == 1
    # 按涨幅降序: 工商银行(3.3) 排最前
    assert codes[0] == '601398'


def test_market_style_snapshot_spread(monkeypatch):
    """风格快照: 防御均 vs 进攻均, spread 符号正确。mock 板块指数涨跌接口。"""
    gate = StyleGate()
    fake_pcts = {
        'BK1666': -0.08,  # 大盘价值(防御)
        'BK1641': -0.96,  # 红利股(防御)
        'BK1139': -0.69,  # 中特估(防御)
        'BK1717': -2.60,  # 题材股(进攻)
        'BK1668': -3.28,  # 小盘价值(进攻)
    }
    monkeypatch.setattr('tools.style_gate.fetch_board_index_pct',
                        lambda bk: fake_pcts.get(bk))
    snap = gate.market_style_snapshot()
    assert snap['defensive_avg'] is not None
    assert snap['aggressive_avg'] is not None
    # 防御均值 = (-0.08 + -0.96 + -0.69)/3 = -0.576...
    assert abs(snap['defensive_avg'] - (-0.58)) < 0.01
    # 进攻均值 = (-2.60 + -3.28)/2 = -2.94
    assert abs(snap['aggressive_avg'] - (-2.94)) < 0.01
    # spread = 防御 - 进攻 = 2.36 (防御占优)
    assert snap['spread'] > 0
    assert abs(snap['spread'] - 2.36) < 0.01


def test_defensive_candidates_handles_network_failure(monkeypatch):
    """网络失败时防御候选返回空列表, 不抛异常。"""
    gate = StyleGate()
    monkeypatch.setattr('tools.style_gate.fetch_board_members',
                        lambda bk, limit=80: [])
    cands = gate.defensive_candidates(top_n=5)
    assert cands == []
