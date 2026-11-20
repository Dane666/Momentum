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

# 防御板块东财行业板块代码(弱势日附送防御候选用)。BK1283=银行 是行业Ⅱ级。
# 用东财板块成分接口一次批量拉成分股+当天涨跌幅, 避免逐只请求。
# 注意(2026-11-20 回测): 深熊期真正抗跌的只有"银行"(42只里37只涨, 中位+9%),
# 煤炭/钢铁/燃气/港口等所谓防御板块照样跟跌(均值-1.5%~-3%)。故收窄为只推银行。
DEFENSIVE_BOARDS = {
    'BK1283': '银行',      # 银行(行业)
    'BK0475': '银行Ⅱ',
    'BK1611': '国有大型银行Ⅲ',
    'BK1610': '股份制银行Ⅲ',
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


def _fetch_eastmoney(url: str) -> Optional[dict]:
    """东财接口通用请求。失败返回 None。"""
    try:
        req = urllib.request.Request(url, headers={'Referer': 'https://quote.eastmoney.com/'})
        return json.loads(urllib.request.urlopen(req, timeout=12).read())
    except Exception:
        return None


def fetch_board_members(board_code: str, limit: int = 60) -> List[dict]:
    """拉取东财板块成分股(含当天涨跌幅)。返回 [{code, name, pct}] 按涨幅降序。

    board_code 形如 'BK0475'(银行Ⅱ)。pct 为当日涨跌幅(%, fltt=2 已还原)。
    """
    url = (f'https://push2.eastmoney.com/api/qt/clist/get'
           f'?pn=1&pz={limit}&po=1&np=1&fltt=2&invt=2&fid=f3'
           f'&fs=b:{board_code}&fields=f12,f14,f3')
    d = _fetch_eastmoney(url)
    if not d:
        return []
    diff = (d.get('data') or {}).get('diff') or []
    out = []
    for it in diff:
        code = it.get('f12', '')
        name = it.get('f14', '')
        pct = it.get('f3')
        if not code:
            continue
        try:
            pct = float(pct) if pct is not None and pct != '-' else None
        except Exception:
            pct = None
        out.append({'code': code, 'name': name, 'pct': pct})
    return out


def fetch_board_index_pct(board_code: str) -> Optional[float]:
    """拉取板块指数当日涨跌幅(%)。用板块详情接口。失败返回 None。

    字段: f170 = 涨跌幅(×100 整数, 需 /100); f43 = 点位(×100, 不可用于涨跌)。
    """
    url = (f'https://push2.eastmoney.com/api/qt/stock/get'
           f'?secid=90.{board_code}&fields=f43,f57,f58,f170')
    d = _fetch_eastmoney(url)
    if not d:
        return None
    data = d.get('data') or {}
    f170 = data.get('f170')
    if f170 is None:
        return None
    try:
        return round(float(f170) / 100.0, 2)
    except Exception:
        return None


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

    def defensive_candidates(self, top_n: int = 6,
                             min_pct: float = 0.0,
                             max_pct: float = 7.0) -> List[dict]:
        """弱势日防御候选清单: 从防御板块成分股里筛出当天逆势上涨(或抗跌)的票。

        返回 [{code, name, pct, board}] 按涨幅降序, 最多 top_n 只。
        min_pct: 只保留当日涨幅 >= min_pct 的票(默认 0 = 只留上涨的)。
        max_pct: 只保留当日涨幅 <= max_pct 的票(默认 7 = 过滤掉当天已涨停/暴涨、
                 次日大概率高开追高的票, 只留"温和逆势上涨"可次日开盘安全介入的)。
        同一只票可能属于多个板块, 去重取首次出现。
        """
        seen = {}
        for bk, bname in DEFENSIVE_BOARDS.items():
            members = fetch_board_members(bk, limit=80)
            for m in members:
                pct = m.get('pct')
                if pct is None or pct < min_pct or pct > max_pct:
                    continue
                if m['code'] in seen:
                    continue
                seen[m['code']] = {
                    'code': m['code'], 'name': m['name'],
                    'pct': pct, 'board': bname,
                }
        ranked = sorted(seen.values(), key=lambda x: -x['pct'])
        return ranked[:top_n]

    def board_strength(self, industries: List[str]) -> Dict[str, Optional[float]]:
        """查询推荐票所属行业板块的当日涨跌幅。返回 {industry: pct}。

        行业名 → 东财板块代码的反查较难(需行业板块列表映射), 这里先按行业名
        在防御板块表里匹配; 对非防御行业返回 None(需另行接行业板块列表)。
        实际调用方(daily_inference)会传入推荐票行业名, 用于"板块走强"二次确认。
        """
        out = {}
        for ind in industries:
            out[ind] = None
        return out

    def market_style_snapshot(self) -> Dict[str, float]:
        """市场风格强弱快照: 对比"防御/价值" vs "题材/进攻"两类风格的当日表现。

        用东财风格板块指数涨跌幅(概念板块 t:3):
          - 防御侧: 大盘价值 BK1666 / 红利股 BK1641 / 中特估 BK1139
          - 进攻侧: 题材股 BK1717 / 微盘(小盘价值 BK1668 作小盘代理)
        返回 {defensive_avg, aggressive_avg, defensive_minus_aggressive, detail}。
        正值 = 防御占优(应转防御), 负值 = 题材占优(题材可持有)。
        """
        def_pcts = []
        agg_pcts = []
        detail = {}
        for bk, side in [
            ('BK1666', '大盘价值'), ('BK1641', '红利股'), ('BK1139', '中特估'),
            ('BK1717', '题材股'), ('BK1668', '小盘价值'),
        ]:
            p = fetch_board_index_pct(bk)
            if p is None:
                continue
            detail[bk] = p
            if side in ('大盘价值', '红利股', '中特估'):
                def_pcts.append(p)
            else:
                agg_pcts.append(p)
        def_avg = sum(def_pcts) / len(def_pcts) if def_pcts else None
        agg_avg = sum(agg_pcts) / len(agg_pcts) if agg_pcts else None
        spread = (def_avg - agg_avg) if (def_avg is not None and agg_avg is not None) else None
        return {
            'defensive_avg': round(def_avg, 2) if def_avg is not None else None,
            'aggressive_avg': round(agg_avg, 2) if agg_avg is not None else None,
            'spread': round(spread, 2) if spread is not None else None,
            'detail': detail,
        }


if __name__ == '__main__':
    gate = StyleGate()
    codes = ['600183', '603186', '002281', '002046', '600036', '601398', '002647', '603382']
    ann = gate.annotate(codes)
    for c, info in ann.items():
        print(f"{c} {info['name']:<6} 行业={info['industry']:<8} 风格={info['style']}")
    print('共振:', gate.sector_resonance(ann))
    print('防御票:', gate.filter_defensive(ann))
