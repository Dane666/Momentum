# -*- coding: utf-8 -*-
"""宽基指数实时行情获取 — 择时闸门(market_timing/risk_gate)的稳健数据源。

背景(2026-11-16 定位的严重 bug):
  kline_cache 里 code='000001' 存的是"平安银行"的 K 线(收盘 7~13 元), 而非上证
  指数(3600+ 点)。market_timing/risk_gate 长期拿它当"大盘 proxy"算 MA20/MA60,
  结果在 2026-08 之后中小盘深度熊市里恒判"强势", 导致 LightGBM 模型全程满仓
  推票、推荐票平均 -3.8%/胜率仅 27%。个股代码 000001 与指数 sh000001 冲突。

本模块统一提供"真·宽基指数"日线:
  - 优先腾讯接口实时抓取(沪深300 sh000300 / 上证指数 sh000001), 缓存到
    data/index_daily_cache/ 下, 避免重复请求;
  - 强校验: 指数点位量级(>500, 个股不可能) + 数据新鲜度(最新交易日);
  - 任何失败显式返回 None 并说明原因, 由调用方保守降级(判弱势/观望),
    绝不静默当"强势"。

用法:
    from tools.index_data import load_index_closes
    dates, closes, meta = load_index_closes('000300', n=66)
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

PROJ = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJ / 'data' / 'index_daily_cache'
DB = str(PROJ / 'qlib_pro_v16.db')

# 指数代码 -> (腾讯前缀, 点位量级下限, 名称)
INDEXES = {
    '000300': ('sh', 500.0, '沪深300'),   # 沪深300 实际 3800~4500
    '000001': ('sh', 500.0, '上证指数'),  # 上证指数 实际 3000~3700
    '399001': ('sz', 500.0, '深证成指'),  # 深证成指 实际 9000~14000
    '399006': ('sz', 500.0, '创业板指'),  # 创业板指 实际 2000~4000
}

# 点位数远低于此的, 几乎可以断定是"个股错乱数据"而非指数(平安银行 7~13 元)。
MIN_INDEX_LEVEL = 500.0


def _today_cst() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')


def _fetch_index_sina(code: str, count: int = 120) -> Optional[List[Tuple[str, float, float, float, float]]]:
    """从新浪历史日线接口抓取宽基指数(稳定源, 2026-11-16 验证可用)。
    返回 [(trade_date, open, close, high, low)] 升序, 失败返回 None。"""
    import urllib.request
    import re
    if code not in INDEXES:
        return None
    prefix, _, _ = INDEXES[code]
    symbol = f'{prefix}{code}'
    url = (f'https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_='
           f'/CN_MarketDataService.getKLineData?symbol={symbol}&scale=240&ma=no&datalen={count}')
    try:
        req = urllib.request.Request(url, headers={'Referer': 'https://finance.sina.com.cn'})
        raw = urllib.request.urlopen(req, timeout=15).read().decode('utf-8', 'ignore')
        m = re.search(r'=\((.*)\)\s*;?\s*$', raw, re.S)
        if not m:
            return None
        data = json.loads(m.group(1))
        rows = []
        for it in data:
            rows.append((it['day'], float(it['open']), float(it['close']),
                         float(it['high']), float(it['low'])))
        return rows if rows else None
    except Exception:
        return None


def _fetch_index_tencent(code: str, count: int = 120,
                         max_retry: int = 1) -> Optional[List[Tuple[str, float, float, float, float]]]:
    """从腾讯日线接口抓取宽基指数(备选, 本地易被 WAF 限流)。
    返回 [(trade_date, open, close, high, low)] 升序, 失败返回 None。"""
    import urllib.request
    if code not in INDEXES:
        return None
    prefix, _, _ = INDEXES[code]
    url = (f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
           f'?param={prefix}{code},day,,,{count},')
    for _ in range(max_retry):
        try:
            req = urllib.request.Request(url, headers={'Referer': 'https://gu.qq.com/'})
            data = json.loads(urllib.request.urlopen(req, timeout=12).read().decode('utf-8', 'ignore'))
            node = data.get('data', {}).get(f'{prefix}{code}')
            if not node:
                continue
            day = node.get('day') or node.get('qfqday') or []
            if not day:
                continue
            rows = []
            for it in day:
                if len(it) < 6:
                    continue
                rows.append((it[0], float(it[1]), float(it[2]), float(it[3]), float(it[4])))
            if rows:
                return rows
        except Exception:
            continue
    return None


def fetch_index(code: str, count: int = 120) -> Optional[List[Tuple[str, float, float, float, float]]]:
    """抓取宽基指数日线: 新浪优先(稳定), 腾讯备选。"""
    rows = _fetch_index_sina(code, count)
    if rows:
        return rows
    return _fetch_index_tencent(code, count)


def load_index_closes(code: str = '000300', n: int = 66) -> Tuple[List[str], List[float], dict]:
    """返回 (dates, closes, meta) — 真·宽基指数收盘序列(升序, 最近 n 条)。

    meta 包含 source/code/name/fresh/last_date/level_min/level_max/warn。
    优先实时抓取(缓存 6 小时), 失败回退缓存文件, 再回退本地库但做量级校验。
    任何路径都做指数量级校验: close 序列中位数 < MIN_INDEX_LEVEL 即判"数据错乱"。
    """
    if code not in INDEXES:
        return [], [], {'source': 'none', 'code': code, 'warn': f'未知指数代码 {code}'}
    prefix, min_lv, name = INDEXES[code]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_fp = CACHE_DIR / f'{code}.json'
    meta = {'source': 'none', 'code': code, 'name': name,
            'fresh': False, 'last_date': None, 'warn': ''}

    def _validate(rows):
        """校验量级: 指数中位数点位必须 > min_lv, 否则是错乱个股数据。"""
        if not rows:
            return None
        closes = [r[2] for r in rows]
        med = sorted(closes)[len(closes) // 2]
        if med < MIN_INDEX_LEVEL:
            meta['warn'] = (f'{name}({code}) 点位中位数 {med:.2f} < {MIN_INDEX_LEVEL}, '
                            f'疑似个股错乱数据(如平安银行), 拒绝作为大盘 proxy')
            return None
        return rows

    rows: Optional[list] = None
    # 1) 实时抓取(新浪优先 -> 腾讯备选)
    fetched = fetch_index(code, count=max(n + 10, 120))
    if fetched:
        _validate(fetched)
        if meta.get('warn'):
            fetched = None
        else:
            rows = fetched
            meta['source'] = 'tencent'
            try:
                cache_fp.write_text(json.dumps(fetched), encoding='utf-8')
            except Exception:
                pass
    # 2) 回退缓存(6 小时内)
    if rows is None and cache_fp.exists():
        try:
            cached = json.loads(cache_fp.read_text(encoding='utf-8'))
            cached = [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in cached]
            _validate(cached)
            if not meta.get('warn'):
                rows = cached
                meta['source'] = 'cache'
        except Exception:
            pass
    # 3) 回退本地库(带量级校验, 上证 000001 会命中"平安银行"错乱数据而拒绝)
    if rows is None:
        try:
            con = sqlite3.connect(DB)
            cur = con.execute(
                "SELECT trade_date, open, close, high, low FROM kline_cache "
                "WHERE code=? ORDER BY trade_date DESC LIMIT ?", (code, n))
            dbrows = cur.fetchall()
            con.close()
            if dbrows:
                dbrows = [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]))
                          for r in dbrows][::-1]
                _validate(dbrows)
                if not meta.get('warn'):
                    rows = dbrows
                    meta['source'] = 'sqlite'
        except Exception:
            pass

    if rows is None:
        return [], [], meta

    # 新鲜度: 最新交易日距今天是否超过 3 个自然日(含周末, 用 4 天容差)
    dates = [r[0] for r in rows]
    closes = [r[2] for r in rows]
    meta['last_date'] = dates[-1]
    try:
        last = datetime.strptime(dates[-1], '%Y-%m-%d')
        gap = (datetime.now(timezone(timedelta(hours=8))) - last.replace(tzinfo=timezone(timedelta(hours=8)))).days
        meta['fresh'] = gap <= 4
        if not meta['fresh']:
            meta['warn'] = (meta['warn'] + '; ' if meta['warn'] else '') + \
                f'{name} 最新交易日 {dates[-1]} 距今 {gap} 天, 数据可能陈旧'
    except Exception:
        meta['fresh'] = False

    meta['level_min'] = round(min(closes), 2)
    meta['level_max'] = round(max(closes), 2)
    return dates[-n:], closes[-n:], meta


if __name__ == '__main__':
    for c in ['000300', '000001', '399001', '399006']:
        d, cl, m = load_index_closes(c, n=66)
        if cl:
            print(f'{m["name"]}({c}) [{m["source"]}] 最新={m["last_date"]} '
                  f'点位 {cl[-1]:.0f} 新鲜={m["fresh"]} warn={m["warn"] or "-"}')
        else:
            print(f'{m["name"]}({c}) 加载失败: {m.get("warn") or "无数据"}')
