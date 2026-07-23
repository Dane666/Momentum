# -*- coding: utf-8 -*-
"""
竞价辅助外盘强弱 — 辅助判断 A 股开盘

1) 碳酸锂期货主力合约 9:00-9:25 走势 (广期所 LC 主连)
   → 辅助判断锂矿/盐湖提锂股开盘强弱
2) 韩股早盘至 09:25 (三星电子 005930.KS / SK 海力士 000660.KS)
   → 辅助判断存储 / HBM / 半导体链开盘强弱

附加: 抓取相关 A 股「今开 vs 昨收」算高开%, 对照外盘方向给出
      「正反馈确认 / 背离」联动提示, 帮助判断外盘信号是否在 A 股兑现。

所有数据获取均 graceful degrade: 任一项失败仅提示, 不影响主竞价报告。
相关 A 股清单见同目录 auction_extra_config.json (可自由增删, 无需改代码)。
"""
import json
import logging
import os
import requests

logger = logging.getLogger('auction_extra')

# ---------- 相关 A 股清单 (默认值, 优先读 auction_extra_config.json) ----------
LITHIUM_STOCKS = [  # 受碳酸锂期货价格直接影响
    ('002466', '天齐锂业'), ('002460', '赣锋锂业'), ('000792', '盐湖股份'),
    ('002738', '中矿资源'), ('002240', '盛新锂能'), ('002192', '融捷股份'),
    ('002756', '永兴材料'), ('002497', '雅化集团'), ('002176', '江特电机'),
    ('000762', '西藏矿业'), ('300390', '天华新能'), ('600499', '科达制造'),
]
KOREA_STOCKS = [  # 受三星 / 海力士 (HBM / 存储) 走势影响
    ('688008', '澜起科技'), ('603986', '兆易创新'), ('300223', '北京君正'),
    ('002049', '紫光国微'), ('603501', '韦尔股份'), ('600584', '长电科技'),
    ('002371', '北方华创'), ('688012', '中微公司'), ('300604', '长川科技'),
    ('000021', '深科技'), ('688981', '中芯国际'),
]

# 正反馈联动阈值(优先读 auction_extra_config.json -> feedback, 缺失回退默认)
FEEDBACK = dict(open_high=0.5, open_low=-0.5, ext_strong=1.0,
                ext_weak=-1.0, linkage_strong=0.7, linkage_mid=0.4)

_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'auction_extra_config.json')


def _load_config():
    """读取外部配置; 缺失/损坏则回退内置默认, 不影响主流程."""
    try:
        with open(_CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        ls = cfg.get('lithium_stocks')
        ks = cfg.get('korea_stocks')
        if ls:
            LITHIUM_STOCKS[:] = [(str(c), str(n)) for c, n in ls]
        if ks:
            KOREA_STOCKS[:] = [(str(c), str(n)) for c, n in ks]
        fb = cfg.get('feedback')
        if isinstance(fb, dict):
            for k in FEEDBACK:
                if k in fb:
                    try:
                        FEEDBACK[k] = float(fb[k])
                    except (TypeError, ValueError):
                        pass
        logger.info('auction_extra 配置已加载: 锂矿%d只 / 存储%d只 / 正反馈阈值 open±%.2f%% ext±%.2f%%',
                    len(LITHIUM_STOCKS), len(KOREA_STOCKS),
                    FEEDBACK['open_high'], FEEDBACK['ext_strong'])
    except FileNotFoundError:
        logger.info('auction_extra_config.json 不存在, 用内置默认清单')
    except Exception as e:
        logger.warning('auction_extra 配置读取失败, 用内置默认: %s', e)


_load_config()

_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://quote.eastmoney.com/'}
_EM_TOKEN = 'D43BF7224DB9CFCF6C6D5D1C95F7E473'


# ============ 碳酸锂期货 ============
def _em_search_secid(keyword: str):
    """动态解析 eastmoney secid (如 '225.lcm')."""
    try:
        url = 'https://searchapi.eastmoney.com/api/suggest/get'
        params = {'input': keyword, 'type': 14, 'token': _EM_TOKEN, 'size': 10}
        r = requests.get(url, params=params, headers=_HEADERS, timeout=12)
        j = r.json()
        for it in (j.get('QuotationCodeTable', {}).get('Data', []) or []):
            name = it.get('Name', '')
            qid = it.get('QuoteID') or it.get('secid')
            if qid and keyword.replace('主连', '') in name:
                return qid
    except Exception as e:
        logger.warning('碳酸锂 secid 搜索失败: %s', e)
    return None


def _em_quote(secid: str):
    try:
        url = 'https://push2.eastmoney.com/api/qt/stock/get'
        params = {'secid': secid, 'fields': 'f43,f44,f45,f46,f60,f170',
                  'fltt': '2', 'invt': '2'}
        r = requests.get(url, params=params, headers=_HEADERS, timeout=12)
        d = r.json().get('data')
        if not d or d.get('f43') in (None, '-', ''):
            return None
        return {k: d.get(k) for k in ('f43', 'f44', 'f45', 'f46', 'f60', 'f170')}
    except Exception as e:
        logger.warning('碳酸锂 quote 获取失败: %s', e)
        return None


def _ef_lithium():
    """efinance 期货实时 (项目已有依赖, CI 美区直连可用)."""
    try:
        import efinance as ef
        df = ef.futures.get_realtime_quotes()
        mask = df.astype(str).apply(
            lambda r: r.str.contains('碳酸锂|LC', case=False, na=False).any(), axis=1)
        sub = df[mask]
        if sub.empty:
            return None
        row = None
        for _, r in sub.iterrows():
            if '主连' in str(r.get('名称', '')):
                row = r
                break
        if row is None:
            row = sub.iloc[0]

        def pick(names):
            for n in names:
                for c in row.index:
                    if n in str(c):
                        try:
                            return float(row[c])
                        except (TypeError, ValueError):
                            return None
            return None

        price = pick(['最新价', '现价', '价格'])
        chg = pick(['涨跌幅'])
        open_ = pick(['今开', '开盘'])
        high = pick(['最高'])
        low = pick(['最低'])
        if price is None:
            return None
        return dict(price=price, chg=chg, open=open_, high=high, low=low)
    except Exception as e:
        logger.warning('efinance 碳酸锂失败: %s', e)
        return None


def fetch_lithium_futures():
    """返回 {name, price, chg, open, high, low, intraday} 或 None."""
    q = _em_quote(_em_search_secid('碳酸锂主连') or '225.lcm')
    if q:
        try:
            price = float(q['f43']); open_ = float(q['f44'])
            high = float(q['f45']); low = float(q['f46'])
            chg = float(q['f170'])
            intraday = (price / open_ - 1) * 100 if open_ else None
            return dict(name='碳酸锂主连(LC)', price=price, chg=chg,
                        open=open_, high=high, low=low, intraday=intraday)
        except (TypeError, ValueError) as e:
            logger.warning('碳酸锂字段解析失败: %s', e)
    # 备用: efinance
    ef = _ef_lithium()
    if ef:
        intraday = (ef['price'] / ef['open'] - 1) * 100 if ef.get('open') else None
        return dict(name='碳酸锂主连(LC)', price=ef['price'], chg=ef.get('chg'),
                    open=ef.get('open'), high=ef.get('high'), low=ef.get('low'),
                    intraday=intraday)
    return None


# ============ 韩股 ============
def fetch_korea_stocks():
    """返回 {三星: {...}, 海力士: {...}} 或空 dict."""
    try:
        import yfinance as yf
    except Exception as e:
        logger.warning('yfinance 不可用: %s', e)
        return {}
    out = {}
    for label, sym in (('三星电子', '005930.KS'), ('SK海力士', '000660.KS')):
        try:
            h = yf.Ticker(sym).history(period='2d', interval='1d')
            if h is None or len(h) == 0:
                continue
            last = h.iloc[-1]
            prev = h.iloc[-2]['Close'] if len(h) >= 2 else last['Open']
            price = float(last['Close'])
            open_ = float(last['Open'])
            chg = (price / prev - 1) * 100 if prev else None
            out[label] = dict(price=price, open=open_, chg=chg,
                              currency='KRW')
        except Exception as e:
            logger.warning('%s 获取失败: %s', label, e)
    return out


# ============ 相关 A 股「高开」(复用主扫描实时行情) ============
def group_open_chg_from_df(stocks, df):
    """从已抓取的全市场实时行情 DataFrame 计算相关 A 股「今开 vs 昨收」高开%.

    零额外网络: 直接复用竞价主扫描已获取的行情(新浪/efinance, 不受 eastmoney 限流).
    列名兼容 efinance(今开/昨收) 与 新浪(今开/昨日收盘). 失败返回空 dict(优雅降级).
    """
    if df is None or getattr(df, 'empty', True):
        return {}
    try:
        import pandas as pd
    except Exception:
        return {}
    cols = list(df.columns)
    code_col = '股票代码' if '股票代码' in cols else None
    open_col = '今开' if '今开' in cols else None
    prev_col = '昨收' if '昨收' in cols else ('昨日收盘' if '昨日收盘' in cols else None)
    if not (code_col and open_col and prev_col):
        logger.info('实时行情缺少 今开/昨收 列, 跳过A股高开反馈')
        return {}
    res = {}
    try:
        sub = df[[code_col, open_col, prev_col]].copy()
        sub[code_col] = sub[code_col].astype(str).str.zfill(6)
        o = pd.to_numeric(sub[open_col], errors='coerce')
        p = pd.to_numeric(sub[prev_col], errors='coerce')
        for c, _ in stocks:
            mask = sub[code_col] == c
            if not mask.any():
                continue
            idx = sub.index[mask][0]
            ov = o.loc[idx]; pv = p.loc[idx]
            if pd.notna(ov) and pd.notna(pv) and pv > 0:
                res[c] = (ov / pv - 1) * 100
    except Exception as e:
        logger.warning('A股高开从DataFrame计算失败: %s', e)
        return {}
    return res


def _feedback_text(ext_chg, open_map, stocks):
    """对照外盘方向, 给出 A 股相关股高开的「正反馈/背离」联动提示.
    阈值全部来自 FEEDBACK 配置(见 auction_extra_config.json)."""
    if not open_map:
        return None  # 拿不到 A 股开盘, 不提示(优雅降级)
    code2name = {c: n for c, n in stocks}
    vals = [v for k, v in open_map.items() if k in code2name]
    if not vals:
        return None
    oh = FEEDBACK['open_high']; ol = FEEDBACK['open_low']
    es = FEEDBACK['ext_strong']; ew = FEEDBACK['ext_weak']
    avg = sum(vals) / len(vals)
    n_high = sum(1 for v in vals if v > oh)
    n_low = sum(1 for v in vals if v < ol)
    ext_up = ext_chg is not None and ext_chg >= es
    ext_down = ext_chg is not None and ext_chg <= ew
    if ext_up and avg > 0:
        return (f"✅ 正反馈确认: 外盘利多已在A股开盘兑现 — 相关股均值 {avg:+.2f}%, "
                f"{n_high}/{len(vals)} 只高开")
    if ext_up and avg <= 0:
        return (f"⚠️ 背离: 外盘利多但A股相关股未跟随 — 均值 {avg:+.2f}%, "
                f"{n_low}/{len(vals)} 只低开, 谨慎追高")
    if ext_down and avg < 0:
        return (f"✅ 负反馈确认: 外盘利空已在A股开盘兑现 — 相关股均值 {avg:+.2f}%, "
                f"{n_low}/{len(vals)} 只低开")
    if ext_down and avg >= 0:
        return (f"⚠️ 背离: 外盘利空但A股相关股逆势 — 均值 {avg:+.2f}%, "
                f"关注个股独立性")
    return f"ℹ️ 外盘中性, A股相关股开盘均值 {avg:+.2f}%"


def _linkage_text(ext_chg, open_map, stocks):
    """板块联动强度评分: 相关 A 股中与外盘同向(外盘涨则跟涨/外盘跌则跟跌)的占比.

    返回形如 "📊 板块联动强度: 🔴强 (4/5 同向, 均值+0.90%)" 的字符串, 或 None.
    强度阈值来自 FEEDBACK.linkage_strong / linkage_mid 配置.
    """
    if not open_map:
        return None
    code2name = {c: n for c, n in stocks}
    items = [(k, v) for k, v in open_map.items() if k in code2name]
    if not items:
        return None
    total = len(items)
    avg = sum(v for _, v in items) / total
    if ext_chg is not None and abs(ext_chg) >= 0.01:
        # 有外盘方向: 同向 = (外盘涨且 A 股涨) 或 (外盘跌且 A 股跌)
        same = sum(1 for _, v in items
                   if (v > 0 and ext_chg > 0) or (v < 0 and ext_chg < 0))
        ratio = same / total
        tag = '同向' if ext_chg >= 0 else '反向(背离方向)'
    else:
        # 外盘中性: 用相关股自身合力方向(涨/跌占比)衡量板块一致性
        up = sum(1 for _, v in items if v > 0)
        down = sum(1 for _, v in items if v < 0)
        same = max(up, down)
        ratio = same / total
        tag = '普涨' if up >= down else '普跌'
    if ratio >= FEEDBACK['linkage_strong']:
        label = '🔴 强'
    elif ratio >= FEEDBACK['linkage_mid']:
        label = '🟡 中'
    else:
        label = '🟢 弱'
    return (f"📊 板块联动强度: {label} ({same}/{total} {tag}, "
            f"均值 {avg:+.2f}%)")


# ============ 文案 ============
def _trend_tag(chg):
    if chg is None:
        return '中性'
    if chg >= 1:
        return '🔴 偏强(利多)'
    if chg <= -1:
        return '🟢 偏弱(利空)'
    return '⚪ 中性'


def build_extra_sections(df=None):
    """生成外部盘辅助段落; 无可数据时返回空串.

    df: 竞价主扫描已抓取的全市场实时行情(供计算相关A股高开正反馈); 为 None 时
        跳过正反馈提示(优雅降级).
    """
    blocks = []

    # --- 碳酸锂 ---
    lc = fetch_lithium_futures()
    lith_line = '🔋 相关锂矿股: ' + ' '.join(f'{n}({c})' for c, n in LITHIUM_STOCKS)
    if lc:
        chg = lc.get('chg')
        intraday = lc.get('intraday')
        tag = _trend_tag(chg)
        parts = [f"🌐 碳酸锂期货主连(LC): {lc['price']:.0f}  涨跌幅 {chg:+.2f}%" if chg is not None
                 else f"🌐 碳酸锂期货主连(LC): {lc['price']:.0f}"]
        if intraday is not None:
            parts.append(f"9:00-9:25 日内 {intraday:+.2f}%")
        parts.append(f"→ 锂矿股开盘参考: {tag}")
        block = '\n'.join(parts) + '\n  ' + lith_line
        # 高开正反馈联动 (复用主扫描行情, 无额外网络)
        fb = _feedback_text(chg, group_open_chg_from_df(LITHIUM_STOCKS, df), LITHIUM_STOCKS)
        if fb:
            block += '\n  ' + fb
        lk = _linkage_text(chg, group_open_chg_from_df(LITHIUM_STOCKS, df), LITHIUM_STOCKS)
        if lk:
            block += '\n  ' + lk
        blocks.append(block)
    else:
        blocks.append('🌐 碳酸锂期货: 数据获取失败(跳过)\n  ' + lith_line)

    # --- 韩股 ---
    kr = fetch_korea_stocks()
    korea_line = '🧠 相关存储/HBM·半导体: ' + ' '.join(f'{n}({c})' for c, n in KOREA_STOCKS)
    if kr:
        seg = []
        up = down = 0
        for label, d in kr.items():
            chg = d.get('chg')
            seg.append(f"{label} {chg:+.2f}%" if chg is not None else f"{label} n/a")
            if chg is not None:
                up += chg > 0
                down += chg < 0
        avg_kr = sum(d['chg'] for d in kr.values() if d.get('chg') is not None)
        avg_kr /= max(1, sum(1 for d in kr.values() if d.get('chg') is not None))
        tag = '🔴 偏强(利多存储链)' if up >= 2 else ('🟢 偏弱(利空存储链)' if down >= 2 else '⚪ 中性')
        block = ('🇰🇷 韩股早盘(至09:25): ' + ' | '.join(seg) + f"  → {tag}"
                 + '\n  ' + korea_line)
        fb = _feedback_text(avg_kr, group_open_chg_from_df(KOREA_STOCKS, df), KOREA_STOCKS)
        if fb:
            block += '\n  ' + fb
        lk = _linkage_text(avg_kr, group_open_chg_from_df(KOREA_STOCKS, df), KOREA_STOCKS)
        if lk:
            block += '\n  ' + lk
        blocks.append(block)
    else:
        blocks.append('🇰🇷 韩股早盘: 数据获取失败(跳过)\n  ' + korea_line)

    return '\n'.join(blocks)
