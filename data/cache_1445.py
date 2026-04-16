# -*- coding: utf-8 -*-
"""
14:45 数据缓存模块

专门缓存回测所需的14:45时刻数据，避免存储完整的5分钟K线。

每条记录仅包含:
- code: 股票代码
- trade_date: 交易日期
- price_1445: 14:45价格
- amount_1445: 截止14:45累计成交额
- volume_1445: 截止14:45累计成交量

相比5分钟K线缓存，存储空间减少约99%。
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from threading import Lock
import logging

logger = logging.getLogger('momentum')

# 写入锁
_write_lock = Lock()


def get_db_path():
    """获取数据库路径"""
    from .. import config as cfg
    return cfg.DB_PATH


def init_1445_cache_table():
    """初始化14:45数据缓存表"""
    db_path = get_db_path()
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS data_1445_cache (
                code TEXT,
                trade_date TEXT,
                price_1445 REAL,
                amount_1445 REAL,
                volume_1445 REAL,
                open_1445 REAL,
                high_1445 REAL,
                low_1445 REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (code, trade_date)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_1445_date ON data_1445_cache(trade_date)")
        conn.commit()
        conn.close()
        logger.debug("[Cache] 14:45数据缓存表已就绪")
    except Exception as e:
        logger.error(f"[Cache] 创建14:45缓存表失败: {e}")


def save_1445_data(code: str, trade_date: str, data: dict) -> bool:
    """
    保存单条14:45数据到缓存
    
    Args:
        code: 股票代码
        trade_date: 交易日期 (YYYY-MM-DD)
        data: 14:45数据字典
        
    Returns:
        是否成功
    """
    if not data:
        return False
    
    db_path = get_db_path()
    
    try:
        with _write_lock:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO data_1445_cache 
                (code, trade_date, price_1445, amount_1445, volume_1445, open_1445, high_1445, low_1445)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code,
                trade_date,
                data.get('price_1445', 0),
                data.get('amount_1445', 0),
                data.get('volume_1445', 0),
                data.get('open_1445', 0),
                data.get('high_1445', 0),
                data.get('low_1445', 0),
            ))
            conn.commit()
            conn.close()
        return True
    except Exception as e:
        logger.warning(f"[Cache] 保存14:45数据失败 {code}/{trade_date}: {e}")
        return False


def save_1445_data_batch(records: List[tuple]) -> int:
    """
    批量保存14:45数据
    
    Args:
        records: [(code, trade_date, data_dict), ...]
        
    Returns:
        成功保存的记录数
    """
    if not records:
        return 0
    
    db_path = get_db_path()
    
    try:
        with _write_lock:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            
            rows = []
            for code, trade_date, data in records:
                if data:
                    rows.append((
                        code,
                        trade_date,
                        data.get('price_1445', 0),
                        data.get('amount_1445', 0),
                        data.get('volume_1445', 0),
                        data.get('open_1445', 0),
                        data.get('high_1445', 0),
                        data.get('low_1445', 0),
                    ))
            
            if rows:
                c.executemany("""
                    INSERT OR REPLACE INTO data_1445_cache 
                    (code, trade_date, price_1445, amount_1445, volume_1445, open_1445, high_1445, low_1445)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
            
            conn.commit()
            conn.close()
            
        logger.debug(f"[Cache] 批量保存 {len(rows)} 条14:45数据")
        return len(rows)
    except Exception as e:
        logger.warning(f"[Cache] 批量保存14:45数据失败: {e}")
        return 0


def load_1445_data(code: str, trade_date: str) -> Optional[dict]:
    """
    从缓存加载单条14:45数据
    
    Args:
        code: 股票代码
        trade_date: 交易日期 (YYYY-MM-DD)
        
    Returns:
        数据字典或None
    """
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        c = conn.cursor()
        c.execute("""
            SELECT price_1445, amount_1445, volume_1445, open_1445, high_1445, low_1445
            FROM data_1445_cache
            WHERE code = ? AND trade_date = ?
        """, (code, trade_date))
        
        row = c.fetchone()
        conn.close()
        
        if row:
            return {
                'price_1445': row[0],
                'amount_1445': row[1],
                'volume_1445': row[2],
                'open_1445': row[3],
                'high_1445': row[4],
                'low_1445': row[5],
            }
        return None
    except Exception as e:
        logger.warning(f"[Cache] 加载14:45数据失败: {e}")
        return None


def load_1445_data_batch(codes: List[str], dates: List[str]) -> Dict[tuple, dict]:
    """
    批量加载14:45数据
    
    Args:
        codes: 股票代码列表
        dates: 日期列表 (YYYY-MM-DD)
        
    Returns:
        {(code, date): data_dict} 字典
    """
    if not codes or not dates:
        return {}
    
    db_path = get_db_path()
    result = {}
    
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        
        # 构建查询
        code_placeholders = ','.join(['?' for _ in codes])
        date_placeholders = ','.join(['?' for _ in dates])
        
        query = f"""
            SELECT code, trade_date, price_1445, amount_1445, volume_1445, 
                   open_1445, high_1445, low_1445
            FROM data_1445_cache
            WHERE code IN ({code_placeholders}) AND trade_date IN ({date_placeholders})
        """
        
        c = conn.cursor()
        c.execute(query, codes + dates)
        
        for row in c.fetchall():
            key = (row[0], row[1])
            result[key] = {
                'price_1445': row[2],
                'amount_1445': row[3],
                'volume_1445': row[4],
                'open_1445': row[5],
                'high_1445': row[6],
                'low_1445': row[7],
            }
        
        conn.close()
        logger.debug(f"[Cache] 批量加载 {len(result)} 条14:45数据")
        return result
    except Exception as e:
        logger.warning(f"[Cache] 批量加载14:45数据失败: {e}")
        return {}


def get_cached_dates(code: str) -> List[str]:
    """获取某只股票已缓存的日期列表"""
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        c = conn.cursor()
        c.execute("""
            SELECT trade_date FROM data_1445_cache
            WHERE code = ? ORDER BY trade_date
        """, (code,))
        
        dates = [row[0] for row in c.fetchall()]
        conn.close()
        return dates
    except Exception as e:
        return []


def get_cache_stats() -> dict:
    """获取14:45缓存统计信息"""
    db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        c = conn.cursor()
        
        c.execute("SELECT COUNT(*) FROM data_1445_cache")
        total_records = c.fetchone()[0]
        
        c.execute("SELECT COUNT(DISTINCT code) FROM data_1445_cache")
        total_codes = c.fetchone()[0]
        
        c.execute("SELECT COUNT(DISTINCT trade_date) FROM data_1445_cache")
        total_dates = c.fetchone()[0]
        
        conn.close()
        
        return {
            'total_records': total_records,
            'total_codes': total_codes,
            'total_dates': total_dates,
        }
    except Exception as e:
        return {'error': str(e)}


def clear_1445_cache(before_date: str = None):
    """
    清理14:45缓存
    
    Args:
        before_date: 清理此日期之前的数据，为None则清理全部
    """
    db_path = get_db_path()
    
    try:
        with _write_lock:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            
            if before_date:
                c.execute("DELETE FROM data_1445_cache WHERE trade_date < ?", (before_date,))
            else:
                c.execute("DELETE FROM data_1445_cache")
            
            deleted = c.rowcount
            conn.commit()
            conn.close()
            
        logger.info(f"[Cache] 清理14:45缓存: {deleted} 条记录")
    except Exception as e:
        logger.error(f"[Cache] 清理14:45缓存失败: {e}")


# 初始化表
init_1445_cache_table()
