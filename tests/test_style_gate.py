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

from tools.style_gate import classify_style, StyleGate, DEFENSIVE_INDUSTRIES, AGGRESSIVE_INDUSTRIES


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
