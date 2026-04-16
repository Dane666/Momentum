# -*- coding: utf-8 -*-
"""
K线缓存模块 v2
- 本地缓存优先加载
- 增量更新
- 线程安全
- 数据源标识
"""

import sqlite3
import pandas as pd
from datetime import datetime
from typing import Callable, Optional
from threading import Lock
import logging

logger = logging.getLogger('momentum')

# 全局写入锁，防止多线程并发写入 SQLite 冲突
_cache_write_lock = Lock()

# 缓存统计
_cache_stats = {'hits': 0, 'misses': 0, 'updates': 0}


def get_cache_stats() -> dict:
    """获取缓存统计信息"""
    return _cache_stats.copy()


def reset_cache_stats():
    """重置缓存统计"""
    global _cache_stats
    _cache_stats = {'hits': 0, 'misses': 0, 'updates': 0}


def get_db_path():
    """获取数据库路径"""
    from .. import config as cfg
    return cfg.DB_PATH


def load_or_fetch_kline(
    code: str,
    fetch_func: Callable[[str, str], pd.DataFrame],
    start_date: str = None
) -> Optional[pd.DataFrame]:
    """
    优先从本地缓存加载K线，缺失部分从API补充（线程安全）

    Args:
        code: 股票代码
        fetch_func: API获取函数 (code, start_date) -> DataFrame
        start_date: 起始日期

    Returns:
        DataFrame with K-line data, or None if failed
    """
    from .. import config as cfg
    start_date = start_date or cfg.KLINE_START_DATE
    db_path = get_db_path()

    try:
        # 读取可以并发，使用 check_same_thread=False
        conn = sqlite3.connect(db_path, check_same_thread=False)

        # 1. 尝试从缓存读取 (使用参数化查询防止SQL注入)
        cached_df = pd.read_sql_query(
            "SELECT * FROM kline_cache WHERE code=? AND trade_date >= ? ORDER BY trade_date",
            conn,
            params=(code, start_date)
        )

        # 2. 判断是否需要更新
        today_str = datetime.now().strftime('%Y-%m-%d')
        current_hour = datetime.now().hour
        current_minute = datetime.now().minute
        
        # 判断是否已收盘（15:00之后）
        is_after_market_close = current_hour > 15 or (current_hour == 15 and current_minute >= 5)
        
        if not cached_df.empty:
            last_cached_date = cached_df['trade_date'].iloc[-1]
            if last_cached_date >= today_str:
                # 【修复】收盘后如果缓存的是盘中数据，需要刷新
                # 通过判断缓存时间来决定是否刷新：盘中缓存的数据在收盘后需要更新
                if is_after_market_close:
                    # 收盘后，重新获取今日数据以确保是收盘价
                    logger.debug(f"[Cache] {code} 收盘后刷新今日数据")
                else:
                    conn.close()
                    _cache_stats['hits'] += 1
                    logger.debug(f"[Cache] {code} 命中缓存 ({len(cached_df)} 条)")
                    return cached_df

        # 3. 从API获取最新数据
        _cache_stats['misses'] += 1
        api_df = fetch_func(code, start_date)
        if api_df is None or api_df.empty:
            conn.close()
            return cached_df if not cached_df.empty else None

        # 4. 标准化字段名
        api_df = api_df.rename(columns={
            'trade_date': 'trade_date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume',
            'amount': 'amount',
            'turnover_ratio': 'turnover_ratio'
        })
        api_df['code'] = code

        # 5. 增量写入新数据 (使用锁确保线程安全)
        if not cached_df.empty:
            last_cached = cached_df['trade_date'].iloc[-1]
            new_data = api_df[api_df['trade_date'] >= last_cached]
        else:
            new_data = api_df

        if not new_data.empty:
            cols = ['code', 'trade_date', 'open', 'high', 'low', 'close',
                   'volume', 'amount', 'turnover_ratio']
            available_cols = [col for col in cols if col in new_data.columns]
            
            # 使用锁保护写入操作
            with _cache_write_lock:
                c = conn.cursor()
                # 删除即将更新的日期数据，避免唯一约束冲突
                for date in new_data['trade_date'].unique():
                    c.execute("DELETE FROM kline_cache WHERE code=? AND trade_date=?", (code, date))
                conn.commit()
                
                # 写入新数据
                new_data[available_cols].to_sql(
                    'kline_cache', conn, if_exists='append', index=False
                )
            _cache_stats['updates'] += 1
            logger.debug(f"[Cache] {code} 更新 {len(new_data)} 条K线记录")

        conn.close()
        return api_df

    except Exception as e:
        logger.warning(f"[Cache] {code} K线加载失败: {e}")
        # 降级: 直接从API获取
        return fetch_func(code, start_date)


def clear_kline_cache(code: str = None):
    """
    清除K线缓存

    Args:
        code: 股票代码，为None时清除所有缓存
    """
    db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        if code:
            c.execute("DELETE FROM kline_cache WHERE code=?", (code,))
            logger.info(f"[Cache] 已清除 {code} 的K线缓存")
        else:
            c.execute("DELETE FROM kline_cache")
            logger.info("[Cache] 已清除所有K线缓存")

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[Cache] 清除缓存失败: {e}")


# ==================== 回测快照缓存 ====================

def create_backtest_snapshot(snapshot_id: str, codes: list) -> bool:
    """
    创建回测数据快照
    
    冻结指定股票的K线数据，确保回测可复现。
    快照创建后，即使后续K线数据更新，回测仍使用快照数据。
    
    Args:
        snapshot_id: 快照唯一标识 (如 "backtest_2026-01-29")
        codes: 需要快照的股票代码列表
        
    Returns:
        是否成功
    """
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # 创建快照表 (如果不存在)
        c.execute("""
            CREATE TABLE IF NOT EXISTS kline_snapshots (
                snapshot_id TEXT,
                code TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                turnover_ratio REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_id, code, trade_date)
            )
        """)
        
        # 从 kline_cache 复制数据到快照
        copied = 0
        for code in codes:
            c.execute("""
                INSERT OR REPLACE INTO kline_snapshots 
                    (snapshot_id, code, trade_date, open, high, low, close, volume, amount, turnover_ratio)
                SELECT ?, code, trade_date, open, high, low, close, volume, amount, turnover_ratio
                FROM kline_cache WHERE code = ?
            """, (snapshot_id, code))
            copied += c.rowcount
        
        conn.commit()
        conn.close()
        
        logger.info(f"[Snapshot] 创建快照 {snapshot_id}: {len(codes)} 只股票, {copied} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"[Snapshot] 创建快照失败: {e}")
        return False


def load_from_snapshot(snapshot_id: str, code: str) -> Optional[pd.DataFrame]:
    """
    从快照加载K线数据
    
    Args:
        snapshot_id: 快照标识
        code: 股票代码
        
    Returns:
        K线 DataFrame 或 None
    """
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        
        df = pd.read_sql_query(
            """SELECT code, trade_date, open, high, low, close, volume, amount, turnover_ratio
               FROM kline_snapshots 
               WHERE snapshot_id = ? AND code = ? 
               ORDER BY trade_date""",
            conn,
            params=(snapshot_id, code)
        )
        
        conn.close()
        
        if df.empty:
            return None
        
        logger.debug(f"[Snapshot] 从快照 {snapshot_id} 加载 {code}: {len(df)} 条")
        return df
        
    except Exception as e:
        logger.warning(f"[Snapshot] 加载快照失败: {e}")
        return None


def list_snapshots() -> list:
    """列出所有快照"""
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        c.execute("""
            SELECT snapshot_id, 
                   COUNT(DISTINCT code) as stock_count,
                   COUNT(*) as record_count,
                   MIN(created_at) as created_at
            FROM kline_snapshots 
            GROUP BY snapshot_id
            ORDER BY created_at DESC
        """)
        
        results = []
        for row in c.fetchall():
            results.append({
                'snapshot_id': row[0],
                'stock_count': row[1],
                'record_count': row[2],
                'created_at': row[3]
            })
        
        conn.close()
        return results
        
    except Exception as e:
        logger.error(f"[Snapshot] 列出快照失败: {e}")
        return []


def delete_snapshot(snapshot_id: str) -> bool:
    """删除指定快照"""
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("DELETE FROM kline_snapshots WHERE snapshot_id = ?", (snapshot_id,))
        deleted = c.rowcount
        conn.commit()
        conn.close()
        
        logger.info(f"[Snapshot] 删除快照 {snapshot_id}: {deleted} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"[Snapshot] 删除快照失败: {e}")
        return False


# ==================== 5分钟K线缓存 (用于14:45精确选股) ====================

def init_5min_kline_table():
    """初始化5分钟K线缓存表"""
    db_path = get_db_path()
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS kline_5min_cache (
                code TEXT,
                trade_time TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (code, trade_time)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_5min_code_date ON kline_5min_cache(code, trade_date)")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[Cache] 创建5分钟K线表失败: {e}")


def load_or_fetch_5min_kline(
    code: str,
    fetch_func,
    start_date: str = None
) -> Optional[pd.DataFrame]:
    """
    优先从本地缓存加载5分钟K线，缺失部分从API补充
    
    Args:
        code: 股票代码
        fetch_func: API获取函数 (code, start_date) -> DataFrame
        start_date: 起始日期
        
    Returns:
        5分钟K线 DataFrame
    """
    from .. import config as cfg
    from datetime import datetime, timedelta
    
    # 默认获取最近30天的5分钟数据
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    db_path = get_db_path()
    
    # 确保表存在
    init_5min_kline_table()
    
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        
        # 1. 尝试从缓存读取
        cached_df = pd.read_sql_query(
            """SELECT * FROM kline_5min_cache 
               WHERE code=? AND trade_date >= ? 
               ORDER BY trade_time""",
            conn,
            params=(code, start_date)
        )
        
        # 2. 判断是否需要更新 (检查是否有今天的数据)
        today_str = datetime.now().strftime('%Y-%m-%d')
        if not cached_df.empty:
            last_cached_date = cached_df['trade_date'].iloc[-1]
            if last_cached_date >= today_str:
                conn.close()
                _cache_stats['hits'] += 1
                logger.debug(f"[Cache] {code} 5分钟K线命中缓存 ({len(cached_df)} 条)")
                return cached_df
        
        # 3. 从API获取最新数据
        _cache_stats['misses'] += 1
        api_df = fetch_func(code, start_date)
        if api_df is None or api_df.empty:
            conn.close()
            return cached_df if not cached_df.empty else None
        
        # 4. 标准化字段
        api_df = api_df.copy()
        api_df['code'] = code
        
        # 确保 trade_date 字段存在
        if 'trade_date' not in api_df.columns and 'trade_time' in api_df.columns:
            api_df['trade_date'] = pd.to_datetime(api_df['trade_time']).dt.strftime('%Y-%m-%d')
        
        # 5. 增量写入新数据
        if not cached_df.empty:
            last_cached_time = cached_df['trade_time'].iloc[-1]
            new_data = api_df[api_df['trade_time'] > last_cached_time]
        else:
            new_data = api_df
        
        if not new_data.empty:
            cols = ['code', 'trade_time', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']
            available_cols = [col for col in cols if col in new_data.columns]
            
            with _cache_write_lock:
                # 使用 INSERT OR REPLACE 避免重复
                new_data[available_cols].to_sql(
                    'kline_5min_cache', conn, if_exists='append', index=False,
                    method='multi'
                )
            _cache_stats['updates'] += 1
            logger.debug(f"[Cache] {code} 更新 {len(new_data)} 条5分钟K线")
        
        conn.close()
        return api_df
        
    except Exception as e:
        logger.warning(f"[Cache] {code} 5分钟K线加载失败: {e}")
        return fetch_func(code, start_date)


def get_1445_data_from_cache(code: str, target_date: str) -> Optional[dict]:
    """
    从缓存中直接获取14:45时刻的数据
    
    Args:
        code: 股票代码
        target_date: 目标日期 (YYYY-MM-DD)
        
    Returns:
        dict with 14:45 data or None
    """
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        
        # 查询目标日期的所有5分钟K线
        df = pd.read_sql_query(
            """SELECT * FROM kline_5min_cache 
               WHERE code=? AND trade_date=? 
               ORDER BY trade_time""",
            conn,
            params=(code, target_date)
        )
        conn.close()
        
        if df.empty:
            return None
        
        # 使用 fetcher 中的提取函数
        from . import extract_1445_data
        return extract_1445_data(df, target_date)
        
    except Exception as e:
        logger.warning(f"[Cache] 获取14:45数据失败: {e}")
        return None
