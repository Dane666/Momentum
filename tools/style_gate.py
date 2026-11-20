# -*- coding: utf-8 -*-
"""弱势日风格闸门 + 推荐票行业识别。

背景(2026-11-20 实证):
  8-25~11-13 中小盘深熊期, 同期收益对比:
    - 银行(防御红利)      +11.5%  胜率100%
    - 红利/高股息(防御)   +9.4%   胜率100%
    - 大盘价值(上证50)    -3.8%   胜率33%
    - 芯片/科技(高弹性)   -20.9%  胜率17%
    - 题材妖股(模型偏好)  -30.1%  胜率0%
  LightGBM 模型是"小盘题材股横截面动量", 从不推防御股(51天推荐0只银行/红利),
  弱势日继续推题材必然亏损。本模块提供:
    1. 行业识别(东财申万行业 f127) —— 给推荐票打真实行业标签
    2. 风格判定(防御 vs 进攻) —— 弱势日过滤题材, 保留防御
    3. 板块共振信号 —— 每日推荐票同行业≥2 视为主线确认

用法:
    from tools.style_gate import classify_stocks, StyleGate
    gate = StyleGate()
    info = gate.annotate(['600183', '600036', ...])  # -> {code: {industry, style, ...}}
"""
from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

PROJ = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJ / 'data' / 'industry_cache'
CACHE_FILE = CACHE_DIR / 'industry_map.json'

# 防御性申万行业(弱势日应保留): 银行/保险/公用事业/红利类
DEFENSIVE_INDUSTRIES = {
    '银行', '银行Ⅱ', '国有大型银行', '股份制银行', '城商行', '农商行',
    '保险', '保险Ⅱ', '证券', '证券Ⅱ',
    '电力', '电力Ⅱ', '火力发电', '水力发电', '燃气', '水务',
    '煤炭', '煤炭开采', '石油', '石油开采', '油气',
    '高速公路', '铁路', '港口', '机场',
    '白酒', '食品饮料', '白色家电', '小家电',
    '水泥', '钢铁',
}

# 高弹性进攻行业(弱势日应过滤): 科技/题材/高波动
AGGRESSIVE_INDUSTRIES = {
    '半导体', '元件', '光学光电子', '消费电子', '电子化学品',
    '通信设备', '通信服务', '计算机设备', '软件开发', 'IT服务',
    '游戏', '互联网', '传媒', '影视',
    '电池', '光伏设备', '风电设备', '锂电',
    '军工', '国防军工', '航天', '航空装备',
    '化学制品', '小金属', '工业金属', '能源金属',
    '化学纤维', '塑料', '橡胶', '通用设备', '专用设备',
    '汽车零部件', '汽车整车',
}


def _fetch_industry_eastmoney(code: str) -> Optional[dict]:
    """东财接口取单只票行业(申万 f127)、地域(f128)、概念(f129)。失败返回 None。"""
    m = '1' if code.startswith(('60', '68', '90')) else '0'
    url = (f'https://push2.eastmoney.com/api/qt/stock/get'
           f'?secid={m}.{code}&fields=f57,f58,f127,f128,f129')
    try:
        req = urllib.request.Request(url, headers={'Referer': 'https://quote.eastmoney.com/'})
        d = json.loads(urllib.request.urlopen(req, timeout=10).read())
        data = d.get('data') or {}
        if not data:
            return None
        return {
            'code': data.get('f57', code),
            'name': data.get('f58', ''),
            'industry': data.get('f127', ''),   # 申万行业
            'region': data.get('f128', ''),      # 地域板块
            'concepts': data.get('f129', ''),    # 概念标签(逗号分隔)
        }
    except Exception:
        return None


def classify_style(industry: str) -> str:
    """按申万行业判定风格: defensive(防御) / aggressive(进攻) / neutral(中性)。"""
    if not industry:
        return 'unknown'
    if industry in DEFENSIVE_INDUSTRIES:
        return 'defensive'
    if industry in AGGRESSIVE_INDUSTRIES:
        return 'aggressive'
    return 'neutral'


class StyleGate:
    """弱势日风格闸门 + 行业注释。带本地缓存(industry_map.json)。"""

    def __init__(self):
        self._cache: Dict[str, dict] = {}
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        if CACHE_FILE.exists():
            try:
                self._cache = json.loads(CACHE_FILE.read_text(encoding='utf-8'))
            except Exception:
                self._cache = {}

    def _load(self, code: str) -> Optional[dict]:
        if code in self._cache:
            return self._cache[code]
        info = _fetch_industry_eastmoney(code)
        if info:
            self._cache[code] = info
            try:
                CACHE_FILE.write_text(json.dumps(self._cache, ensure_ascii=False), encoding='utf-8')
            except Exception:
                pass
        return info

    def annotate(self, codes: List[str]) -> Dict[str, dict]:
        """给一批票打行业+风格标签。返回 {code: {name, industry, region, style}}。"""
        out = {}
        for c in codes:
            info = self._load(c)
            if info:
                out[c] = {
                    'name': info.get('name', ''),
                    'industry': info.get('industry', ''),
                    'region': info.get('region', ''),
                    'style': classify_style(info.get('industry', '')),
                }
            else:
                out[c] = {'name': '', 'industry': '', 'region': '', 'style': 'unknown'}
        return out

    def sector_resonance(self, annotated: Dict[str, dict]) -> Dict[str, int]:
        """统计行业共振: 同行业(非空)票数。返回 {industry: count}。"""
        from collections import Counter
        cnt = Counter()
        for info in annotated.values():
            ind = info.get('industry', '')
            if ind:
                cnt[ind] += 1
        return dict(cnt)

    def filter_defensive(self, annotated: Dict[str, dict], keep_style=('defensive',)) -> List[str]:
        """弱势日: 只保留指定风格(默认防御)的票。返回保留的 code 列表。"""
        return [c for c, info in annotated.items() if info.get('style') in keep_style]


if __name__ == '__main__':
    gate = StyleGate()
    codes = ['600183', '603186', '002281', '002046', '600036', '601398', '002647', '603382']
    ann = gate.annotate(codes)
    for c, info in ann.items():
        print(f"{c} {info['name']:<6} 行业={info['industry']:<8} 风格={info['style']}")
    print('共振:', gate.sector_resonance(ann))
    print('防御票:', gate.filter_defensive(ann))
