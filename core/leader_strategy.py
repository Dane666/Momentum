# -*- coding: utf-8 -*-
"""
龙头策略 — 热门行业资金流入 + 涨幅最强龙头股选取 + MA60 闸口
================================================================
对标 opt_study:
  harness_c_ma60.py       → build_ma60_gate (L37-49), simulate_with_log_gated (L52-174)
  harness_c_ma60_timing.py → open 分支 (L84-88): gate[T] = T-1收盘站上 MA60[T-1]
  harness_sector.py         → build_sector_heat: 板块资金净流入 top-K
  harness_compare3.py       → build_day_returns + topn_leaders
"""
from __future__ import annotations
import logging
from collections import defaultdict
from typing import Optional, Dict, List, Tuple
import pandas as pd

logger = logging.getLogger('momentum.leader')


def _get_sector_from_code(code: str) -> str:
    """从股票代码获取行业分类 — 优先从 K 线缓存, 降级用板块前缀."""
    try:
        from ..data import load_or_fetch_kline, fetch_kline_from_api
        df = load_or_fetch_kline(str(code), fetch_kline_from_api)
        if df is not None and not df.empty:
            for col in ['sw', 'industry', 'sector']:
                if col in df.columns:
                    val = df[col].dropna()
                    if not val.empty:
                        return str(val.iloc[-1])
    except Exception:
        pass
    prefix = code[:2]
    if prefix == '60': return '上海主板'
    elif prefix == '68': return '科创板'
    elif prefix == '00': return '深圳主板'
    elif prefix == '30': return '创业板'
    return '其它'


def fetch_market_data() -> Optional[pd.DataFrame]:
    """获取全 A 股实时行情 (涨跌幅, 成交额, 最新价)."""
    try:
        import efinance as ef
        df = ef.stock.get_realtime_quotes(fs='沪深A股')
        if df is not None and not df.empty:
            for c in ['涨跌幅', '成交额', '最新价']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            return df
    except Exception as e:
        logger.warning(f"efinance 实时行情获取失败: {e}")
    try:
        from ..data import fetch_all_stock_codes
        from ..data.fetcher import fetch_quotes_sina
        codes = [c for c in fetch_all_stock_codes() if c.startswith(('60', '00', '30', '68'))]
        df = fetch_quotes_sina(codes)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    return None


def calc_sector_heat(
    df_real: pd.DataFrame,
    code_sector_map: Dict[str, str],
    min_stocks: int = 4
) -> Dict[str, float]:
    """
    计算行业热度 — 近似板块资金净流入.

    简化为: sector_heat = sum(成交额 × 涨跌幅%) per sector
    (对标 harness_sector.py stock_net_inflow 口径)
    """
    df = df_real.copy()
    df = df[df['股票代码'].astype(str).str.startswith(('60', '00', '30', '68'))]
    df = df[~df['股票名称'].astype(str).str.contains('ST|退', na=False)]
    df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
    df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
    df['net_flow'] = df['成交额'] * df['涨跌幅'] / 100

    sector_heat = defaultdict(float)
    sector_count = defaultdict(int)
    for _, row in df.iterrows():
        sec = code_sector_map.get(str(row['股票代码']), '其它')
        sector_heat[sec] += row['net_flow']
        sector_count[sec] += 1

    return {s: h for s, h in sector_heat.items() if sector_count.get(s, 0) >= min_stocks}


def select_leaders(
    df_real: pd.DataFrame,
    hot_sectors: List[str],
    code_sector_map: Dict[str, str],
    top_n: int = 3,
    min_change_pct: float = 2.0
) -> List[Dict]:
    """热门行业内按涨幅选取最强 N 只龙头."""
    df = df_real.copy()
    df = df[df['股票代码'].astype(str).str.startswith(('60', '00', '30', '68'))]
    df = df[~df['股票名称'].astype(str).str.contains('ST|退', na=False)]
    df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
    df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
    df['最新价'] = pd.to_numeric(df['最新价'], errors='coerce').fillna(0)
    df['_sector'] = df['股票代码'].astype(str).map(code_sector_map).fillna('其它')

    hot_set = set(hot_sectors)
    df = df[df['_sector'].isin(hot_set)]
    df = df[df['涨跌幅'] >= min_change_pct]
    df = df.sort_values('涨跌幅', ascending=False).head(top_n)

    leaders = []
    for _, r in df.iterrows():
        leaders.append({
            'code': str(r['股票代码']),
            'name': str(r.get('股票名称', '?')),
            'price': round(float(r['最新价']), 2),
            'change_pct': round(float(r['涨跌幅']), 2),
            'sector': str(r['_sector']),
            'amount': round(float(r['成交额']) / 1e8, 2),
        })
    return leaders


def build_sector_map_quick(df_real: pd.DataFrame, cache_size: int = 200) -> Dict[str, str]:
    """快速构建 代码→行业 映射 (采样成交额 Top-N 查 K 线, 其余降级)."""
    sector_map = {}
    df = df_real.copy()
    df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
    df['股票代码'] = df['股票代码'].astype(str)
    top_codes = df.nlargest(cache_size, '成交额')['股票代码'].tolist()
    for code in top_codes:
        sec = _get_sector_from_code(code)
        if sec not in ('其它',):
            sector_map[code] = sec

    def _fallback(c):
        if c in sector_map: return sector_map[c]
        p = c[:2]
        if p == '60': return '上海主板'
        elif p == '68': return '科创板'
        elif p == '00': return '深圳主板'
        elif p == '30': return '创业板'
        return '其它'

    for _, r in df.iterrows():
        code = r['股票代码']
        if code not in sector_map:
            sector_map[code] = _fallback(code)
    return sector_map


def format_report(
    leaders: List[Dict],
    hot_sectors: List[Tuple[str, float]],
    is_ma60_bull: bool,
    ma60_msg: str,
    ma60_detail: dict
) -> str:
    """生成龙头策略推送报告."""
    lines = ['🐉 龙头策略 | 14:44 尾盘']

    if not is_ma60_bull:
        lines.append('⚠️ MA60闸口: 大盘跌破60日线 — 龙头选股暂停, 空仓等待')
        if ma60_detail:
            diff = ma60_detail.get('diff_pct', 0)
            lines.append(f'   T-1收盘 {ma60_detail.get("close",0):.0f} vs '
                         f'MA60 {ma60_detail.get("ma60",0):.0f} ({diff:+.1f}%)')
    else:
        diff = ma60_detail.get('diff_pct', 0) if ma60_detail else 0
        lines.append(f'✅ MA60闸口: 站上60日线 (+{diff:.1f}%) — 可择机建仓')

    lines.append('─' * 40)

    if hot_sectors:
        lines.append(f'🔥 热门行业 (资金净流入 Top{len(hot_sectors)}):')
        for sec, heat in hot_sectors[:8]:
            heat_wan = heat / 1e4
            lines.append(f'  {sec}: {heat_wan:+.0f}万')

    if leaders and is_ma60_bull:
        lines.append(f'\n🚀 龙头选股 (热门行业涨幅 Top{len(leaders)}):')
        for l in leaders:
            lines.append(
                f'  {l["code"]} {l["name"]}: ¥{l["price"]:.2f} '
                f'({l["change_pct"]:+.1f}%) | {l["sector"]} | 成交{l["amount"]:.1f}亿'
            )
    elif not leaders and is_ma60_bull:
        lines.append('\n📋 今日热门行业无符合条件的龙头 (涨幅不足)')
    elif not is_ma60_bull:
        lines.append('\n🛑 大盘空头 — 今日不选股')

    return '\n'.join(lines)


def run_leader_scan() -> Tuple[List[Dict], str, bool]:
    """
    运行龙头策略选股.

    Returns:
        (leaders_list, report_text, is_ma60_bull)
    """
    from .. import config as cfg

    if not getattr(cfg, 'LEADER_STRATEGY_ENABLED', True):
        logger.info("[Leader] 已禁用")
        return [], "龙头策略已禁用", True

    top_k = getattr(cfg, 'LEADER_TOP_K_SECTORS', 8)
    top_n = getattr(cfg, 'LEADER_TOP_N_PICKS', 3)
    min_stocks = getattr(cfg, 'LEADER_MIN_SECTOR_STOCKS', 4)
    min_change = getattr(cfg, 'LEADER_MIN_CHANGE_PCT', 2.0)

    # 1. MA60 闸口
    logger.info("[Leader] Phase 1: MA60 闸口检查...")
    is_ma60_bull, ma60_msg, ma60_detail = True, "跳过", {}
    try:
        from ..factors.market import calc_ma60_gate_open
        is_ma60_bull, ma60_msg, ma60_detail = calc_ma60_gate_open('000001')
    except Exception as e:
        logger.warning(f"[Leader] MA60检查异常: {e}")

    # 2. 实时行情
    logger.info("[Leader] Phase 2: 获取全市场实时行情...")
    df_real = fetch_market_data()
    if df_real is None or df_real.empty:
        logger.error("[Leader] 实时行情获取失败")
        return [], "数据获取失败", is_ma60_bull

    # 3. 行业映射
    logger.info("[Leader] Phase 3: 构建行业映射...")
    code_sector_map = build_sector_map_quick(df_real)

    # 4. 行业热度
    logger.info("[Leader] Phase 4: 计算行业热度...")
    sector_heat = calc_sector_heat(df_real, code_sector_map, min_stocks)
    hot_sectors = sorted(sector_heat.items(), key=lambda x: -x[1])[:top_k]

    if not hot_sectors:
        logger.warning("[Leader] 无热门行业")
        rpt = format_report([], [], is_ma60_bull, ma60_msg, ma60_detail)
        return [], rpt, is_ma60_bull

    hot_sector_names = [s for s, _ in hot_sectors]

    # 5. 龙头选取
    leaders = []
    if is_ma60_bull:
        logger.info(f"[Leader] Phase 5: 在 {len(hot_sector_names)} 个热门行业中选龙头...")
        leaders = select_leaders(df_real, hot_sector_names, code_sector_map, top_n, min_change)
        logger.info(f"[Leader] 选出 {len(leaders)} 只龙头: {[l['code'] for l in leaders]}")
    else:
        logger.info("[Leader] MA60空头, 跳过龙头选取")

    rpt = format_report(leaders, hot_sectors, is_ma60_bull, ma60_msg, ma60_detail)
    return leaders, rpt, is_ma60_bull
