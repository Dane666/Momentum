# -*- coding: utf-8 -*-
"""
数据获取模块
- 统一封装数据爬取接口
- 新浪为主数据源（稳定可靠）
- 腾讯为 K 线备用数据源

=== 数据源规范 (v2 - 2026.01) ===
| 数据类型     | 主数据源    | 备用数据源   | 备注                    |
|-------------|------------|-------------|------------------------|
| 实时行情     | 新浪       | 东财(仅备用) | 新浪稳定，efinance 不稳定 |
| 个股K线      | adata      | 腾讯        | adata 失败时用腾讯       |
| ETF K线      | 腾讯       | -           | 腾讯稳定                |
| 指数K线      | 腾讯       | 东财        | 腾讯优先                |
| 板块概念     | adata      | -           | 统一使用 adata          |
"""

import os
import pandas as pd
from typing import Optional, List
import logging
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 导入代理防护模块（在其他导入之前）
from .proxy_guard import disable_proxy
disable_proxy()

from .sources import (
    get_market_session, 
    is_trading_hours, 
    is_trading_day,
    MarketSession,
    DataSource,
    normalize_kline_df,
)

logger = logging.getLogger('momentum')


def _normalize_stock_codes(codes: List[str]) -> List[str]:
    """标准化股票代码并过滤掉当前行情链路不支持的市场。"""
    normalized = []
    for code in codes:
        if code is None:
            continue
        code_str = str(code).strip()
        if not code_str:
            continue
        if code_str.endswith('.0'):
            code_str = code_str[:-2]
        code_str = code_str.zfill(6)
        # 当前新浪行情链路仅覆盖沪深市场，北交所代码先过滤掉
        if not code_str.startswith(('00', '30', '60', '68')):
            continue
        normalized.append(code_str)
    return list(dict.fromkeys(normalized))


def _apply_code_limit(codes: List[str]) -> List[str]:
    """应用环境变量中的代码数量限制。"""
    limit = os.getenv('MOMENTUM_CODE_LIMIT', '').strip()
    if not limit:
        return codes
    try:
        return codes[:max(1, int(limit))]
    except ValueError:
        return codes


def _read_stock_codes_from_csv(csv_path: str) -> List[str]:
    """从 CSV 读取并标准化股票代码。"""
    df = pd.read_csv(csv_path, dtype={'stock_code': str})
    if df is None or df.empty or 'stock_code' not in df.columns:
        return []

    filtered = df.copy()
    if 'short_name' in filtered.columns:
        filtered = filtered[~filtered['short_name'].astype(str).str.contains('PT', na=False)]
        filtered = filtered[~filtered['short_name'].astype(str).str.contains('退', na=False)]

    return _apply_code_limit(_normalize_stock_codes(filtered['stock_code'].tolist()))


def get_exchange(code: str) -> str:
    """简易交易所推断"""
    if code.startswith(('6', '5')):
        return 'sh'
    elif code.startswith(('0', '3', '1')):
        return 'sz'
    return 'sh'  # Default

def fetch_quotes_sina(codes: list) -> pd.DataFrame:
    """
    使用新浪接口获取实时行情 (Fallback)
    """
    # import adata.common.utils.code_utils as code_utils
    
    # 1. 构建 URL 列表 (分块)
    chunk_size = 80
    chunks = [codes[i:i + chunk_size] for i in range(0, len(codes), chunk_size)]
    urls = []
    
    for chunk in chunks:
        query_list = []
        for code in chunk:
            # exch = code_utils.get_exchange_by_stock_code(code).lower()
            exch = get_exchange(str(code))
            query_list.append(f"{exch}{code}")
        urls.append(f"https://hq.sinajs.cn/list={','.join(query_list)}")
        
    # 2. 并发请求
    data = []
    headers = {'Referer': 'https://finance.sina.com.cn/'}
    
    def fetch_chunk(url):
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                return resp.text
        except Exception:
            pass
        return ""

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_chunk, urls)
        
    # 3. 解析数据
    for text in results:
        if not text:
            continue
        lines = text.strip().split('\n')
        for line in lines:
            if not line:
                continue
            try:
                # var hq_str_sz000001="name,open,..."
                eq_idx = line.find('=')
                if eq_idx == -1:
                    continue
                
                # 解析代码
                # var hq_str_sz000001
                key = line[:eq_idx]
                code = key.split('_')[-1][2:] # sz000001 -> 000001
                
                # 解析内容
                val = line[eq_idx+2:-2] # "..."
                if not val:
                    continue
                    
                parts = val.split(',')
                if len(parts) < 30:
                    continue
                    
                # 提取字段
                name = parts[0]
                open_p = float(parts[1])
                pre_close = float(parts[2])
                price = float(parts[3])
                high = float(parts[4])
                low = float(parts[5])
                volume = float(parts[8])
                amount = float(parts[9])
                
                # 处理 0 价格 (停牌或集合竞价前)
                if price == 0:
                    price = pre_close
                
                change = price - pre_close
                pct = (change / pre_close * 100) if pre_close > 0 else 0
                
                data.append({
                    '股票代码': code,
                    '股票名称': name,
                    '最新价': price,
                    '涨跌幅': pct,
                    '涨跌额': change,
                    '成交量': volume,
                    '成交额': amount,
                    '最高': high,
                    '最低': low,
                    '今开': open_p,
                    '昨日收盘': pre_close,
                    '量比': 1.0, # 新浪不直接提供量比，暂设为 1
                    '换手率': 0.0, # 新浪不提供换手率，暂设为 0
                    '总市值': 0, # 新浪不提供
                })
            except Exception:
                continue
                
    return pd.DataFrame(data)


def is_etf(code: str) -> bool:
    """判断是否为 ETF 代码"""
    return code.startswith(('51', '15', '56', '58'))


def fetch_kline_from_api(code: str, start_date: str) -> Optional[pd.DataFrame]:
    """
    获取 K 线数据 (统一使用腾讯接口，避免数据源切换导致的格式不一致)
    
    数据源规范 (v3 - 固定数据源):
    - ETF: 腾讯 (稳定，无代理问题)
    - 个股: 腾讯 (稳定，无代理问题)
    
    Args:
        code: 股票/ETF代码
        start_date: 开始日期 (YYYY-MM-DD)
    
    Returns:
        标准化后的 K 线 DataFrame
    """
    # 统一使用腾讯接口获取 K 线数据
    # 腾讯接口稳定可靠，无代理问题，字段格式一致
    return _fetch_kline_from_tencent(code, start_date)


def _fetch_kline_from_tencent(code: str, start_date: str, count: int = 500) -> Optional[pd.DataFrame]:
    """
    使用腾讯接口获取 K 线数据 (主数据源)
    
    特点:
    - 接口稳定，无代理问题
    - 支持前复权
    - 字段格式一致
    
    注意:
    - volume 单位是"手"(100股)，已转换为"股"
    - amount 为估算值 (volume * close)
    """
    try:
        # 确定交易所前缀
        if code.startswith('6'):
            prefix = 'sh'
        else:
            prefix = 'sz'
        
        url = f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={prefix}{code},day,,,{count},qfq'
        headers = {'Referer': 'https://gu.qq.com/'}
        
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        
        data = resp.json()
        if not data or 'data' not in data:
            return None
        
        kline_key = f'{prefix}{code}'
        if kline_key not in data['data']:
            return None
        
        # 优先使用 'day'，如果为空尝试 'qfqday'
        day_data = data['data'][kline_key].get('day', [])
        if not day_data:
            day_data = data['data'][kline_key].get('qfqday', [])
        
        if not day_data:
            return None
        
        # 转换为 DataFrame
        # 腾讯格式: [日期, 开盘, 收盘, 最高, 最低, 成交量(手)]
        # 注意: 腾讯返回的成交量单位是"手"(100股)，需要转换
        rows = []
        for item in day_data:
            if len(item) >= 6:
                volume_lot = float(item[5])  # 单位: 手
                volume_share = volume_lot * 100  # 转换为股
                close_price = float(item[2])
                # 估算成交额 = 成交量(股) * 收盘价
                # 注意：实际成交额应该用 (high+low+close)/3 或 VWAP，但收盘价估算也可接受
                estimated_amount = volume_share * close_price
                rows.append({
                    'trade_date': item[0],
                    'open': float(item[1]),
                    'close': close_price,
                    'high': float(item[3]),
                    'low': float(item[4]),
                    'volume': volume_share,  # 单位: 股
                    'amount': estimated_amount,  # 单位: 元
                    'turnover_ratio': 0.0,
                })
        
        if rows:
            df = pd.DataFrame(rows)
            logger.debug(f"[Fetcher] 个股 {code} K线获取成功 (腾讯)")
            return df
        
    except Exception as e:
        logger.debug(f"[Fetcher] 个股 {code} K线获取失败 (腾讯): {e}")
    
    return None


def fetch_all_stock_codes_eastmoney() -> list:
    """
    [Fallback] 直接从东方财富获取全市场 A 股代码列表
    """
    try:
        # 使用通用域名，避免特定节点 (82.push2) 不可用
        # 尝试使用 HTTPS
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "10000",  # 一次性获取所有
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23",
            "fields": "f12",  # f12: 代码
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            logger.warning(f"[Fetcher] Eastmoney Direct HTTP {resp.status_code}")
            return []

        data = resp.json()
        
        if data and 'data' in data and 'diff' in data['data']:
            # data['data']['diff'] 是个 list 或 dict (取决于页码)
            # 当 pz 很大时通常是 list
            diff = data['data']['diff']
            codes = []
            if isinstance(diff, list):
                codes = [item['f12'] for item in diff]
            elif isinstance(diff, dict):
                codes = [item['f12'] for item in diff.values()]
            
            logger.info(f"[Fetcher] Eastmoney Direct 获取到 {len(codes)} 只股票代码")
            return codes
            
    except Exception as e:
        logger.warning(f"[Fetcher] Eastmoney Direct 获取代码列表失败: {e}")
    
    return []


def fetch_all_stock_codes_local() -> list:
    """
    从本地/内置 CSV 文件获取股票代码列表 (离线回退)
    """
    import os
    try:
        candidate_paths = []

        # 1. 显式指定的代码列表，便于 smoke test 或手工调试
        env_csv_path = os.getenv('MOMENTUM_CODE_LIST_FILE', '').strip()
        if env_csv_path:
            candidate_paths.append(env_csv_path)

        # 2. adata 安装包自带的全量代码缓存
        try:
            from adata.stock.cache import get_code_csv_path
            candidate_paths.append(get_code_csv_path())
        except Exception:
            pass

        # 3. 历史兼容路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        legacy_csv_path = os.path.join(current_dir, '..', '..', 'utils', 'all_code.csv')
        candidate_paths.append(os.path.normpath(legacy_csv_path))

        seen_paths = set()
        for csv_path in candidate_paths:
            if not csv_path or csv_path in seen_paths:
                continue
            seen_paths.add(csv_path)

            if not os.path.exists(csv_path):
                logger.warning(f"[Fetcher] 本地代码文件不存在: {csv_path}")
                continue

            codes = _read_stock_codes_from_csv(csv_path)
            if codes:
                logger.info(f"[Fetcher] 本地文件获取到 {len(codes)} 只股票代码: {csv_path}")
                return codes
    except Exception as e:
        logger.warning(f"[Fetcher] 本地代码文件读取失败: {e}")
    
    return []


def fetch_all_stock_codes_adata() -> list:
    """
    使用 adata 动态获取全市场股票代码。

    优先使用 repo 内已有的 adata 能力，避免默认依赖静态 CSV。
    """
    try:
        import adata.stock.info as stock_info

        df = stock_info.all_code()
        if df is None or df.empty:
            logger.warning("[Fetcher] adata 未返回股票代码")
            return []

        code_column = 'stock_code' if 'stock_code' in df.columns else 'code'
        if code_column not in df.columns:
            logger.warning(f"[Fetcher] adata 返回缺少股票代码列: {df.columns.tolist()}")
            return []

        filtered = df.copy()
        if 'short_name' in filtered.columns:
            filtered = filtered[~filtered['short_name'].astype(str).str.contains('PT', na=False)]
            filtered = filtered[~filtered['short_name'].astype(str).str.contains('退', na=False)]

        codes = _apply_code_limit(_normalize_stock_codes(filtered[code_column].tolist()))
        logger.info(f"[Fetcher] adata 获取到 {len(codes)} 只股票代码")
        return codes
    except Exception as e:
        logger.warning(f"[Fetcher] adata 获取代码列表失败: {e}")
        return []


def fetch_all_stock_codes() -> list:
    """
    获取股票代码列表。

    优先级：
    1. 显式指定的本地 CSV（便于人工调试或定向 smoke test）
    2. adata 动态代码列表
    3. 东方财富动态代码列表
    4. 项目内置本地 CSV 兜底
    """
    csv_path = os.getenv('MOMENTUM_CODE_LIST_FILE', '').strip()
    if csv_path:
        return fetch_all_stock_codes_local()

    for getter in (fetch_all_stock_codes_adata, fetch_all_stock_codes_eastmoney, fetch_all_stock_codes_local):
        codes = getter()
        if codes:
            return codes

    return []


def fetch_realtime_quotes(fs: str = '沪深A股') -> Optional[pd.DataFrame]:
    """
    获取实时行情 (新浪优先，更稳定)
    
    数据源优先级：
    1. 新浪接口（主数据源，稳定可靠）
    2. efinance（备用，经常连接失败）
    
    支持的市场类型:
    - 沪深A股: 全市场A股
    - ETF: 全市场ETF
    - 沪股通: 沪港通标的
    - 深股通: 深港通标的  
    
    Args:
        fs: 市场类型
    
    Returns:
        标准化的行情 DataFrame，失败返回空 DataFrame
    """
    import time
    logger.info(f"[Fetcher] 获取 {fs} 实时行情")
    
    # 1. 优先使用新浪接口（稳定可靠）
    # 获取代码列表
    if fs == '沪深A股':
        codes = fetch_all_stock_codes()
    elif fs == 'ETF':
        df_etf = fetch_etf_list()
        codes = df_etf['股票代码'].tolist() if df_etf is not None else []
    elif fs == '沪股通':
        codes = fetch_all_stock_codes()
        codes = [c for c in codes if c.startswith('60')]
    elif fs == '深股通':
        codes = fetch_all_stock_codes()
        codes = [c for c in codes if c.startswith(('00', '30'))]
    else:
        logger.warning(f"[Fetcher] 不支持的市场类型: {fs}")
        return pd.DataFrame()
    
    if codes:
        logger.info(f"[Fetcher] 使用新浪接口获取 {fs} ({len(codes)} 只)")
        try:
            df = fetch_quotes_sina(codes)
            if df is not None and len(df) > 10:
                # 标记量比需要从K线计算
                df['_fake_vol_ratio'] = True
                logger.info(f"[Fetcher] 新浪接口成功: {len(df)} 只 {fs}")
                return df
        except Exception as e:
            logger.warning(f"[Fetcher] 新浪接口失败: {e}，切换到 efinance")
    
    # 2. 备用：efinance（经常连接失败）
    for attempt in range(2):
        try:
            import efinance as ef
            df = ef.stock.get_realtime_quotes(fs=fs)
            if df is not None and len(df) > 10:
                logger.info(f"[Fetcher] efinance 成功: {len(df)} 只 {fs}")
                return df
        except Exception as e:
            if attempt < 1:
                logger.debug(f"[Fetcher] efinance 第{attempt+1}次失败，1秒后重试...")
                time.sleep(1)
            else:
                logger.warning(f"[Fetcher] efinance 也失败: {e}")
    
    logger.error(f"[Fetcher] 所有数据源均失败")
    return pd.DataFrame()


def fetch_stock_concept(code: str, use_cache: bool = True) -> str:
    """
    获取股票所属概念板块 (带缓存)

    Args:
        code: 股票代码
        use_cache: 是否使用缓存 (默认True)

    Returns:
        板块名称，失败返回 "其它"
    """
    # 优先从缓存读取
    if use_cache:
        try:
            import sqlite3
            from datetime import datetime, timedelta
            from . import get_db_connection
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # 读取缓存 (7天内有效)
                cursor.execute(
                    "SELECT sector, update_time FROM stock_sector_cache WHERE code=?",
                    (code,)
                )
                row = cursor.fetchone()
                if row:
                    sector, update_time = row
                    # 检查是否过期 (7天)
                    update_dt = datetime.fromisoformat(update_time)
                    if datetime.now() - update_dt < timedelta(days=7):
                        return sector
        except Exception as e:
            logger.debug(f"[Fetcher] {code} 缓存读取失败: {e}")
    
    # 从网络获取
    sector = "其它"
    try:
        import adata
        plates = adata.stock.info.get_concept_ths(stock_code=code)
        if plates is not None and not plates.empty:
            sector = plates.iloc[0]['name']
    except Exception as e:
        logger.debug(f"[Fetcher] {code} 概念获取失败: {e}")
    
    # 写入缓存
    if use_cache:
        try:
            from datetime import datetime
            from . import get_db_connection
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO stock_sector_cache (code, sector, update_time) VALUES (?, ?, ?)",
                    (code, sector, datetime.now().isoformat())
                )
                conn.commit()
        except Exception as e:
            logger.debug(f"[Fetcher] {code} 缓存写入失败: {e}")
    
    return sector


def fetch_market_index(index_code: str = '000300', k_type: int = 1) -> Optional[pd.DataFrame]:
    """
    获取指数K线 (优先使用腾讯接口)

    Args:
        index_code: 指数代码 (000300=沪深300, 000001=上证指数)
        k_type: K线类型 (1=日线)

    Returns:
        DataFrame with index K-line
    """
    import requests
    
    # 1. 优先使用腾讯接口获取真实历史数据
    try:
        # 沪深300 属于上交所
        if index_code in ('000300', '000001', '000016'):
            prefix = 'sh'
        else:
            prefix = 'sz'
        
        url = f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={prefix}{index_code},day,,,500,qfq'
        headers = {'Referer': 'https://gu.qq.com/'}
        
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and 'data' in data:
                kline_key = f'{prefix}{index_code}'
                if kline_key in data['data']:
                    day_data = data['data'][kline_key].get('day', [])
                    if not day_data:
                        day_data = data['data'][kline_key].get('qfqday', [])
                    
                    if day_data and len(day_data) > 60:
                        rows = []
                        for item in day_data:
                            if len(item) >= 5:
                                rows.append({
                                    'trade_date': item[0],
                                    'open': float(item[1]),
                                    'close': float(item[2]),
                                    'high': float(item[3]),
                                    'low': float(item[4]),
                                    'volume': float(item[5]) if len(item) > 5 else 0,
                                    'amount': 0,
                                })
                        if rows:
                            logger.info(f"[Fetcher] 获取指数 {index_code} 成功 (腾讯): {len(rows)} 天")
                            return pd.DataFrame(rows)
    except Exception as e:
        logger.debug(f"[Fetcher] 腾讯指数接口失败: {e}")
    
    # 2. 回退: 使用东财接口
    secid_map = {
        '000300': '1.000300',  # 沪深300
        '000001': '1.000001',  # 上证指数
        '399001': '0.399001',  # 深证成指
        '399006': '0.399006',  # 创业板指
    }
    secid = secid_map.get(index_code, f'1.{index_code}')
    
    # 使用东财实时行情 API 获取指数最新信息
    try:
        realtime_url = 'http://push2.eastmoney.com/api/qt/stock/get'
        realtime_params = {
            'secid': secid,
            'fields': 'f43,f44,f45,f46,f47,f48,f57,f58,f59,f60,f169,f170',  # 现价、涨跌幅等
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Referer': 'http://quote.eastmoney.com/',
        }
        resp = requests.get(realtime_url, params=realtime_params, headers=headers, timeout=5)
        data = resp.json()
        
        if data.get('data'):
            d = data['data']
            # 构建简化的 DataFrame（只包含最新一天的数据）
            # 对于 ETF 轮动策略，我们主要需要 20 日涨幅，这里通过实时数据计算
            close = float(d.get('f43', 0)) / 100  # f43 是现价，需要除以100
            rows = []
            # 生成模拟的历史数据（用于计算20日涨幅的占位）
            # 增加到 60 天以支持 ADX/ATR 计算
            end_date = datetime.now()
            for i in range(60):
                mock_price = close * (1 - 0.001 * i)
                date = end_date - timedelta(days=60-i)
                rows.append({
                    'trade_date': date.strftime('%Y-%m-%d'),
                    'close': mock_price,
                    'open': mock_price,
                    'high': mock_price,
                    'low': mock_price,
                    'volume': 0,
                    'amount': 0,
                })
            # 最后一条是真实数据
            rows[-1] = {
                'trade_date': end_date.strftime('%Y-%m-%d'),
                'close': close,
                'open': float(d.get('f46', close * 100)) / 100,
                'high': float(d.get('f44', close * 100)) / 100,
                'low': float(d.get('f45', close * 100)) / 100,
                'volume': float(d.get('f47', 0)),
                'amount': float(d.get('f48', 0)),
            }
            logger.info(f"[Fetcher] 获取指数 {index_code} 成功，当前点位: {close:.2f}")
            return pd.DataFrame(rows)
    except Exception as e:
        logger.warning(f"[Fetcher] 获取指数 {index_code} 失败: {e}")
        return None


def fetch_etf_list() -> Optional[pd.DataFrame]:
    """
    获取全市场 ETF 列表 (使用内置列表，避免 py_mini_racer 兼容性问题)

    Returns:
        DataFrame with ETF list (代码, 名称)
    """
    etf_list = [
        # 行业 ETF
        ('515880', '通信ETF'), ('512690', '白酒ETF'), ('512170', '医疗ETF'),
        ('515790', '光伏ETF'), ('159806', '新能源车ETF'), ('512660', '军工ETF'),
        ('159995', '芯片ETF'), ('512480', '半导体ETF'), ('512980', '传媒ETF'),
        ('159819', 'AI ETF'), ('515030', '新能源ETF'), ('159869', '游戏ETF'),
        ('512800', '银行ETF'), ('512880', '证券ETF'), ('512070', '保险ETF'),
        ('512400', '有色ETF'), ('515220', '煤炭ETF'), ('515210', '钢铁ETF'),
        ('159825', '农业ETF'), ('516160', '新能源车ETF'), ('159992', '创新药ETF'),
        ('515120', '创新药沪深港ETF'), ('512010', '医药ETF'), ('515700', '新材料ETF'),
        # 宽基 ETF
        ('510300', '沪深300ETF'), ('510500', '中证500ETF'), ('159915', '创业板ETF'),
        ('510050', '上证50ETF'), ('512100', '中证1000ETF'), ('588000', '科创50ETF'),
        ('159901', '深100ETF'), ('510180', '上证180ETF'), ('159922', '中证500ETF'),
        # 商品 ETF
        ('518880', '黄金ETF'), ('159981', '白银ETF'), ('159985', '豆粕ETF'),
        ('159980', '有色金属ETF'), ('161226', '白银LOF'),
        # 跨境 ETF
        ('513100', '纳指ETF'), ('513500', '标普500ETF'), ('513650', '标普500ETF南方'),
        ('159941', '纳指ETF'), ('513050', '中概互联ETF'), ('513180', '恒生科技ETF'),
        ('159920', '恒生ETF'), ('513060', '恒生医疗ETF'), ('513330', '恒生互联网ETF'),
        # 债券 ETF
        ('511260', '十年国债ETF'), ('511010', '国债ETF'), ('511220', '城投债ETF'),
    ]
    return pd.DataFrame(etf_list, columns=['股票代码', '股票名称'])


def fetch_5min_kline(code: str, start_date: str = None, count: int = 200) -> Optional[pd.DataFrame]:
    """
    获取5分钟K线历史数据 (用于14:45精确选股)
    
    使用东财接口 k_type=5 获取5分钟K线
    
    Args:
        code: 股票代码
        start_date: 开始日期 (YYYY-MM-DD)
        count: 获取条数 (默认200，约40天的5分钟K线)
        
    Returns:
        DataFrame: trade_time, open, high, low, close, volume, amount
        其中 trade_time 格式为 "2026-01-29 14:45:00"
    """
    try:
        # 使用 adata 的东财接口获取5分钟K线
        import adata
        
        # k_type=5 表示5分钟K线
        df = adata.stock.market.get_market(
            stock_code=code, 
            k_type=5,  # 5分钟K线
            adjust_type=1  # 前复权
        )
        
        if df is None or df.empty:
            return None
        
        # 标准化字段名
        df = df.rename(columns={
            'trade_time': 'trade_time',
            'trade_date': 'trade_date'
        })
        
        # 确保有 trade_time 字段
        if 'trade_time' not in df.columns and 'trade_date' in df.columns:
            df['trade_time'] = df['trade_date']
        
        # 过滤日期
        if start_date:
            df['date_only'] = pd.to_datetime(df['trade_time']).dt.date
            start_dt = pd.to_datetime(start_date).date()
            df = df[df['date_only'] >= start_dt]
            df = df.drop(columns=['date_only'])
        
        logger.debug(f"[Fetcher] {code} 5分钟K线获取成功: {len(df)} 条")
        return df
        
    except Exception as e:
        logger.warning(f"[Fetcher] {code} 5分钟K线获取失败: {e}")
        return None


def extract_1445_data(df_5min: pd.DataFrame, target_date: str) -> Optional[dict]:
    """
    从5分钟K线中提取14:45时刻的数据
    
    14:45的5分钟K线代表14:45-14:50这段时间的OHLCV
    实盘在14:45选股时看到的就是这个时刻的价格和累计成交额
    
    Args:
        df_5min: 5分钟K线DataFrame
        target_date: 目标日期 (YYYY-MM-DD)
        
    Returns:
        dict with keys: price_1445, amount_1445, volume_1445, open, high, low
    """
    if df_5min is None or df_5min.empty:
        return None
    
    try:
        # 解析时间
        df = df_5min.copy()
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df['date_str'] = df['trade_time'].dt.strftime('%Y-%m-%d')
        df['time_str'] = df['trade_time'].dt.strftime('%H:%M')
        
        # 过滤目标日期
        day_data = df[df['date_str'] == target_date]
        if day_data.empty:
            return None
        
        # 查找14:45的数据 (5分钟K线时间戳是该K线的开始时间)
        # 14:45时刻对应的K线是 14:45-14:50
        bar_1445 = day_data[day_data['time_str'] == '14:45']
        
        if bar_1445.empty:
            # 如果没有14:45，尝试14:40或14:50
            for fallback_time in ['14:40', '14:50', '14:35']:
                bar_1445 = day_data[day_data['time_str'] == fallback_time]
                if not bar_1445.empty:
                    break
        
        if bar_1445.empty:
            # 最后回退：使用当天最后一根K线（不包括15:00收盘）
            day_data_before_close = day_data[day_data['time_str'] < '15:00']
            if not day_data_before_close.empty:
                bar_1445 = day_data_before_close.iloc[[-1]]
            else:
                return None
        
        # 计算截止14:45的累计成交额（从开盘到14:45）
        day_data_until_1445 = day_data[day_data['time_str'] <= '14:45']
        cumulative_amount = day_data_until_1445['amount'].sum() if 'amount' in day_data_until_1445.columns else 0
        cumulative_volume = day_data_until_1445['volume'].sum() if 'volume' in day_data_until_1445.columns else 0
        
        bar = bar_1445.iloc[0]
        
        return {
            'price_1445': float(bar['close']),  # 14:45时刻的价格
            'open_1445': float(bar['open']),
            'high_1445': float(bar['high']),
            'low_1445': float(bar['low']),
            'amount_1445': float(cumulative_amount),  # 截止14:45的累计成交额
            'volume_1445': float(cumulative_volume),  # 截止14:45的累计成交量
            'time_str': bar_1445.iloc[0]['time_str'],  # 实际使用的时间
        }
        
    except Exception as e:
        logger.warning(f"[Fetcher] 提取14:45数据失败: {e}")
        return None


def fetch_etf_quotes_with_fallback(fs: str = 'ETF') -> Optional[pd.DataFrame]:
    """
    获取 ETF 实时行情，支持盘后回退到 K 线数据

    Args:
        fs: 市场类型

    Returns:
        DataFrame with ETF quotes
    """
    # 尝试获取实时行情
    df = fetch_realtime_quotes(fs=fs)
    if df is not None and not df.empty:
        return df

    logger.info("[Fetcher] 实时行情不可用，尝试使用 K 线数据")

    # 回退方案: 获取 ETF 列表 + 最新 K 线
    df_list = fetch_etf_list()
    if df_list is None or df_list.empty:
        logger.error("[Fetcher] 无法获取 ETF 列表")
        return None

    # 构建行情数据
    from datetime import datetime, timedelta

    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    quotes = []

    for _, row in df_list.iterrows():
        code = str(row['股票代码'])
        name = row['股票名称']

        try:
            # 使用现有的 K 线获取函数
            df_kline = fetch_kline_from_api(code, start_date)
            if df_kline is not None and not df_kline.empty:
                df_kline = df_kline.sort_values('trade_date').reset_index(drop=True)
                df_kline['close'] = pd.to_numeric(df_kline['close'], errors='coerce')
                df_kline['volume'] = pd.to_numeric(df_kline['volume'], errors='coerce')
                df_kline['amount'] = pd.to_numeric(df_kline.get('amount', pd.Series([0])), errors='coerce').fillna(0)

                last = df_kline.iloc[-1]

                # 计算涨跌幅
                if len(df_kline) >= 2:
                    prev_close = float(df_kline.iloc[-2]['close'])
                    close = float(last['close'])
                    change_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
                else:
                    change_pct = 0

                # 计算量比 (今日成交量 / 5日均量)
                vol_5d_avg = df_kline['volume'].iloc[-5:].mean() if len(df_kline) >= 5 else float(last['volume'])
                vol_ratio = float(last['volume']) / vol_5d_avg if vol_5d_avg > 0 else 1.0

                quotes.append({
                    '股票代码': code,
                    '股票名称': name,
                    '最新价': float(last['close']),
                    '涨跌幅': change_pct,
                    '成交量': float(last['volume']),
                    '成交额': float(last['amount']) if 'amount' in last else 0,
                    '量比': vol_ratio,
                    '开盘': float(last.get('open', last['close'])),
                    '最高': float(last.get('high', last['close'])),
                    '最低': float(last.get('low', last['close'])),
                })
        except Exception as e:
            logger.debug(f"[Fetcher] {code} K线获取失败: {e}")
            continue

    if quotes:
        logger.info(f"[Fetcher] 成功获取 {len(quotes)} 只 ETF 行情 (K线回退)")
        return pd.DataFrame(quotes)

    return None


def batch_calculate_vol_ratio(codes: List[str], max_workers: int = 20) -> dict:
    """
    批量计算真实量比 (并行获取K线)
    
    量比 = 当日成交量 / 过去5日平均成交量
    
    Args:
        codes: 股票代码列表
        max_workers: 并发线程数
        
    Returns:
        dict: {code: vol_ratio} 映射
    """
    from datetime import datetime, timedelta
    
    vol_ratio_map = {}
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    def calc_single(code: str) -> tuple:
        try:
            df = fetch_kline_from_api(code, start_date)
            if df is None or len(df) < 6:
                return code, 1.0
            
            # 量比 = 当日成交量 / 过去5日平均成交量
            avg_vol_5 = df['volume'].tail(6).iloc[:-1].mean()
            vol_ratio = df['volume'].iloc[-1] / (avg_vol_5 + 1e-9)
            return code, round(vol_ratio, 2)
        except Exception:
            return code, 1.0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(calc_single, codes)
        for code, vr in results:
            vol_ratio_map[code] = vr
    
    return vol_ratio_map
