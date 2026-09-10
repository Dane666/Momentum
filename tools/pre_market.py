# -*- coding: utf-8 -*-
"""
盘前全景早报 + 开仓评分 (每日 08:30 推送)

四级评分: 外围(美股/A50/恒生) + 宏观(DXY/CNH/美债) + 政策快讯
≥3 积极 | 1-2 谨慎 | 0~-1 多看少动 | ≤-2 空仓

数据源: 新浪 hq.sinajs.cn (稳定, 项目统一使用) —— 替代 yfinance
  yfinance 在 CI 共享 IP 下频繁被限流("Too Many Requests"), 且 XINA50.NYB/
  DX-Y.NYB/CNH=X 等期货外汇代码返回 NaN → 早报出现 "nan%"。
  新浪接口单次批量返回, 无限流, 字段含涨跌额/涨跌幅, 无需二次计算。
"""
import os
import re
import logging
from datetime import datetime
from typing import Optional, Dict, List, Tuple
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('pre_market')

# ---------------------------------------------------------------------------
# 新浪行情代码映射 (hq.sinajs.cn)
# 返回格式 (美股指数/个股):
#   var hq_str_gb_ixic="纳斯达克,26253.3398,-0.64,时间,涨跌额,今开,...";
#   字段: [0]名称 [1]最新价 [2]涨跌幅% [3]时间 [4]涨跌额 [5]今开 [6]昨收 ...
# 返回格式 (恒生 rt_hkHSI):
#   var hq_str_rt_hkHSI="HSI,恒生指数,现价,昨收,今开,...[6]涨跌额 [7]涨跌幅% ...";
# 返回格式 (美元指数 DINIW / 外汇 fx_susdcnh): 字段位置不同, 见各解析函数。
# ---------------------------------------------------------------------------

# 外围指数
OVERSEAS = {
    'gb_inx':   '标普500',
    'gb_ixic':  '纳斯达克',
    'gb_$dji':  '道琼斯',
    'rt_hkHSI': '恒生指数',
}

# 核心科技股 (美股)
TECH = {
    'gb_nvda': '英伟达', 'gb_aapl': '苹果', 'gb_tsla': '特斯拉', 'gb_msft': '微软',
    'gb_amd':  'AMD',    'gb_mu':   '镁光', 'gb_wdc':  '西部数据', 'gb_stx': '希捷',
    'gb_smci': '超微电脑', 'gb_avgo': '博通', 'gb_intc': '英特尔', 'gb_sox': '费城半导体',
}

# 宏观 (美元指数 / 离岸人民币 / 美债10Y)
MACRO = {
    'DINIW':      '美元指数',
    'fx_susdcnh': '离岸人民币',
}

# A50 期指: 新浪无稳定实时代码, 从评分项降级 (用恒指+美股三大指数+美元+人民币已足够)
# 保留占位以便未来补充稳定源

POLICY_POS = ['降准', '降息', '减税', '产业扶持', '回购', '增持', '暂停IPO', '印花税', '促消费']
POLICY_NEG = ['加息', '监管问询', '立案调查', '地缘冲突', '制裁', '关税', '反倾销', '退市']

# 评分项: 注意 A50 期指 / 美债10Y 已降级移除 (无稳定源), 用其余 5 项评分
# 阈值统一为「百分数」口径 (与新浪 change_pct 一致, 如 0.5 表示 +0.5%)
SCORE_CHECKS = [
    ('标普500',  lambda d: _nz(d, 'gb_inx', 'change_pct') > 0.5,   lambda d: _nz(d, 'gb_inx', 'change_pct') < -0.8),
    ('纳斯达克', lambda d: _nz(d, 'gb_ixic', 'change_pct') > 0.5,  lambda d: _nz(d, 'gb_ixic', 'change_pct') < -1.5),
    ('恒生指数', lambda d: _nz(d, 'rt_hkHSI', 'change_pct') > 0.5, lambda d: _nz(d, 'rt_hkHSI', 'change_pct') < -0.8),
    ('美元指数', lambda d: _nz(d, 'DINIW', 'change_pct') < -0.3,   lambda d: _nz(d, 'DINIW', 'change_pct') > 0.3),
    ('人民币',   lambda d: _nz(d, 'fx_susdcnh', 'change_pct') < -0.2, lambda d: _nz(d, 'fx_susdcnh', 'change_pct') > 0.3),
]


def _nz(d: Dict, key: str, field: str) -> float:
    """安全取值: 缺失/NaN 一律返回 0, 避免评分和格式化出现 NaN。"""
    v = d.get(key, {})
    if not isinstance(v, dict):
        return 0.0
    val = v.get(field, 0.0)
    try:
        f = float(val)
        return f if pd.notna(f) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _parse_num(s: str) -> float:
    """解析字符串为 float, 空/非法返回 NaN。"""
    if s is None:
        return float('nan')
    s = str(s).strip()
    if s == '' or s == '--' or s.lower() == 'nan':
        return float('nan')
    try:
        return float(s)
    except (ValueError, TypeError):
        return float('nan')


def fetch_sina_batch(codes: Dict[str, str]) -> Dict[str, Dict]:
    """
    从新浪批量获取行情 (单次请求)。

    返回: {code: {name, close, change, change_pct, date}}
    解析三种返回格式:
      - 美股指数/个股 (gb_*):   [0]名称 [1]价 [2]涨跌幅 [3]时间 [4]涨跌额
      - 恒生 (rt_hkHSI):        [1]名称 [2]现价 [6]涨跌额 [7]涨跌幅 [17]日期
      - 美元指数 (DINIW):       [8]名称 [1]现价 [12]涨跌额? [13]涨跌幅
      - 外汇 (fx_susdcnh):      [8]名称 [1]现价 ... 涨跌幅在尾段
    对无法可靠映射的, 用「今开/昨收」兜底计算涨跌幅。
    """
    try:
        # 新浪 Referer 必须带, 否则 403
        url = 'https://hq.sinajs.cn/list=' + ','.join(codes.keys())
        headers = {'Referer': 'https://finance.sina.com.cn/', 'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logger.warning(f'[PreMarket] sina status {resp.status_code}')
            return {}
        text = resp.text
    except Exception as e:
        logger.warning(f'[PreMarket] sina fetch error: {e}')
        return {}

    result = {}
    for code, display in codes.items():
        try:
            m = re.search(r'hq_str_%s="([^"]*)"' % re.escape(code), text)
            if not m or not m.group(1):
                logger.debug(f'[PreMarket] {code} no data')
                continue
            parts = m.group(1).split(',')
            if len(parts) < 3:
                continue
            d = _parse_sina_parts(code, parts, display)
            if d is not None:
                result[code] = d
        except Exception as e:
            logger.debug(f'[PreMarket] parse {code} error: {e}')
    return result


def _parse_sina_parts(code: str, parts: List[str], display: str) -> Optional[Dict]:
    """按代码类型解析新浪字段。返回 None 表示数据无效。"""
    close = change = change_pct = float('nan')
    name = display

    if code.startswith('gb_'):
        # 美股: [0]名称 [1]最新价 [2]涨跌幅% [3]时间 [4]涨跌额 [5]今开
        name = parts[0] if parts[0] else display
        close = _parse_num(parts[1])
        change_pct = _parse_num(parts[2])  # 已是百分比数值
        change = _parse_num(parts[4])
    elif code.startswith('rt_hk'):
        # 恒生: [1]名称 [2]现价 [7]涨跌额 [8]涨跌幅% [17]日期
        name = parts[1] if len(parts) > 1 and parts[1] else display
        close = _parse_num(parts[2])
        change = _parse_num(parts[7]) if len(parts) > 7 else float('nan')
        change_pct = _parse_num(parts[8]) if len(parts) > 8 else float('nan')
    elif code == 'DINIW':
        # 美元指数: [0]时间 [1]现价 [2]昨收 [6]最高 [7]最低 [9]名称 [10]日期
        name = parts[9] if len(parts) > 9 and parts[9] else display
        close = _parse_num(parts[1])
        # 无涨跌幅字段, 用 parts[2](昨收) 兜底计算
        prev = _parse_num(parts[2])
        if pd.notna(close) and pd.notna(prev) and prev > 0:
            change = close - prev
            change_pct = change / prev * 100
    elif code.startswith('fx_'):
        # 外汇: [0]时间 [1]现价 [8]昨收 [9]名称 [10]涨跌额 [11]涨跌幅(小数, 需×100)
        name = parts[9] if len(parts) > 9 and parts[9] else display
        close = _parse_num(parts[1])
        if len(parts) > 11:
            change = _parse_num(parts[10])
            change_pct = _parse_num(parts[11]) * 100  # 小数 → 百分数
        # 涨跌幅兜底: 用昨收 parts[8]
        if pd.isna(change_pct):
            prev = _parse_num(parts[8])
            if pd.notna(close) and pd.notna(prev) and prev > 0:
                change = close - prev
                change_pct = change / prev * 100
    else:
        return None

    # 有效性校验: 最新价必须为有效正数
    if pd.isna(close) or close <= 0:
        return None

    # change_pct 兜底: 若仍为 NaN 则按 0 处理 (避免 nan%)
    if pd.isna(change_pct):
        change_pct = 0.0
    if pd.isna(change):
        change = 0.0

    return {
        'ticker': code,
        'name': name,
        'close': close,
        'change': change,
        'change_pct': change_pct,
        'date': datetime.now().strftime('%Y-%m-%d'),
    }


def fetch_all() -> Dict:
    """统一从新浪批量拉取外围+科技+宏观。"""
    all_codes = {**OVERSEAS, **TECH, **MACRO}
    return fetch_sina_batch(all_codes)


def scan_policy() -> Tuple[int, List[str]]:
    hits = []
    score = 0
    try:
        resp = requests.get('https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=8.4.6', timeout=8,
                            headers={'User-Agent': 'Mozilla/5.0'})
        if resp.status_code == 200:
            for item in resp.json().get('data', {}).get('roll_data', [])[:30]:
                t = item.get('title', '')
                for kw in POLICY_POS:
                    if kw in t:
                        hits.append(f'+{kw}')
                        score += 1
                for kw in POLICY_NEG:
                    if kw in t:
                        hits.append(f'-{kw}')
                        score -= 1
    except Exception:
        pass
    return min(score, 2), hits[:5]


def score_market(data: Dict) -> Tuple[int, str, List[str]]:
    total = 0
    details = []
    # 致命判定: 美股大跌 + 恒指大跌 (change_pct 已是百分数)
    sp = _nz(data, 'gb_inx', 'change_pct')
    hsi = _nz(data, 'rt_hkHSI', 'change_pct')
    if sp < -2.0 and hsi < -1.0:
        return -3, '🔴 美股大跌+恒指大跌,今日不宜开新仓', [f'标普{sp:.1f}% 恒指{hsi:.1f}%']
    # 逐项评分 (SCORE_CHECKS 内部已用百分数值比较)
    for nm, pos, neg in SCORE_CHECKS:
        if pos(data):
            total += 1
            details.append(f'+{nm}')
        elif neg(data):
            total -= 1
            details.append(f'-{nm}')
    # 宏观同步 (三杀): 美元涨+人民币贬+利率升
    dxy = _nz(data, 'DINIW', 'change_pct')
    cnh = _nz(data, 'fx_susdcnh', 'change_pct')
    if dxy > 0.3 and cnh > 0.3:
        total -= 1
        details.append('-三杀')
    # 政策
    ps, ph = scan_policy()
    total += ps
    details.extend(ph)
    # 判定
    total = max(-3, min(4, total))
    if total >= 3:
        v = '🟢 积极操作'
    elif total >= 1:
        v = '🟡 谨慎参与'
    elif total >= 0:
        v = '⚪ 多看少动'
    else:
        v = '🔴 空仓休息'
    return total, v, details


def fmt(v: float, pct: bool = True) -> str:
    """安全格式化: 非有限值输出 '--', 杜绝 nan%。"""
    try:
        f = float(v)
        if pd.isna(f):
            return '--'
    except (TypeError, ValueError):
        return '--'
    s = '+' if f >= 0 else ''
    return f'{s}{f:.2f}%' if pct else f'{s}{f:.2f}'


def build_report(data: Dict) -> str:
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    total, verdict, details = score_market(data)
    lines = [f'📊 盘前早报 | {now}', f'开仓评分: {total:+d} → {verdict}']
    if details:
        lines.append(f'明细: {",".join(details[:8])}')
    lines.append('─' * 40)
    lines.append('🇺🇸 外围市场')
    for t, n in OVERSEAS.items():
        d = data.get(t)
        if d:
            lines.append(f'  {"🔺" if d["change_pct"] > 0 else "🔻"} {n}: {d["close"]:.2f} ({fmt(d["change_pct"])})')
    lines.append('\n💻 核心科技')
    for t, n in TECH.items():
        d = data.get(t)
        if d:
            lines.append(f'  {"🔺" if d["change_pct"] > 0 else "🔻"} {n}: {d["close"]:.2f} ({fmt(d["change_pct"])})')
    lines.append('\n🌍 宏观')
    for t, n in MACRO.items():
        d = data.get(t)
        if d:
            if t == 'DINIW':
                lines.append(f'  📌 {n}: {d["close"]:.4f} ({fmt(d["change_pct"])})')
            elif t == 'fx_susdcnh':
                lines.append(f'  📌 {n}: {d["close"]:.4f} ({fmt(d["change_pct"])})')
            else:
                lines.append(f'  📌 {n}: {d["close"]:.2f} ({fmt(d["change_pct"])})')
    lines.append(f'\n📅 {datetime.now().strftime("%Y-%m-%d")}')
    lines.append('⏰ 09:25 竞价扫描见')
    return '\n'.join(lines)


def send_notify(text: str):
    from momentum.notify.bark import send_bark
    send_bark('盘前早报', text)


def run():
    logger.info('[PreMarket] fetching (sina)...')
    d = fetch_all()
    if not d:
        return logger.error('no data from sina')
    rpt = build_report(d)
    print(rpt)
    send_notify(rpt)


if __name__ == '__main__':
    run()
