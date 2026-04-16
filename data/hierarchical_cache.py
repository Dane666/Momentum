# -*- coding: utf-8 -*-
"""
分层缓存架构模块

实现 内存(L1) → 磁盘(L2) → 数据库(L3) 三级缓存，
提升数据访问效率，减少IO和网络开销。
"""

import os
import pickle
import hashlib
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable
from threading import Lock, RLock
from functools import lru_cache
from collections import OrderedDict
import logging

logger = logging.getLogger('momentum.cache')


class LRUCache:
    """
    线程安全的LRU内存缓存
    
    用于L1缓存层，存储最近使用的数据
    """
    
    def __init__(self, maxsize: int = 100):
        self.maxsize = maxsize
        self.cache: OrderedDict = OrderedDict()
        self.lock = RLock()
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存项"""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                self.hits += 1
                return self.cache[key]
            self.misses += 1
            return None
    
    def set(self, key: str, value: Any):
        """设置缓存项"""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.maxsize:
                    self.cache.popitem(last=False)
            self.cache[key] = value
    
    def delete(self, key: str):
        """删除缓存项"""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
    
    def clear(self):
        """清空缓存"""
        with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0
    
    @property
    def hit_rate(self) -> float:
        """命中率"""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    def stats(self) -> dict:
        """统计信息"""
        with self.lock:
            return {
                'size': len(self.cache),
                'maxsize': self.maxsize,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': self.hit_rate,
            }


class DiskCache:
    """
    磁盘缓存层
    
    用于L2缓存，将数据序列化到本地文件
    """
    
    def __init__(self, cache_dir: str = None, ttl_hours: int = 24):
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(__file__), '.cache')
        
        self.cache_dir = cache_dir
        self.ttl_hours = ttl_hours
        self.lock = Lock()
        
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_path(self, key: str) -> str:
        """获取缓存文件路径"""
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hash_key}.pkl")
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        path = self._get_path(key)
        
        if not os.path.exists(path):
            return None
        
        try:
            with self.lock:
                # 检查TTL
                mtime = os.path.getmtime(path)
                age_hours = (time.time() - mtime) / 3600
                
                if age_hours > self.ttl_hours:
                    os.remove(path)
                    return None
                
                with open(path, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.warning(f"Disk cache read error: {e}")
            return None
    
    def set(self, key: str, value: Any):
        """设置缓存"""
        path = self._get_path(key)
        
        try:
            with self.lock:
                with open(path, 'wb') as f:
                    pickle.dump(value, f)
        except Exception as e:
            logger.warning(f"Disk cache write error: {e}")
    
    def delete(self, key: str):
        """删除缓存"""
        path = self._get_path(key)
        
        with self.lock:
            if os.path.exists(path):
                os.remove(path)
    
    def clear(self):
        """清空缓存目录"""
        with self.lock:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.pkl'):
                    try:
                        os.remove(os.path.join(self.cache_dir, filename))
                    except Exception:
                        pass
    
    def cleanup_expired(self):
        """清理过期缓存"""
        with self.lock:
            now = time.time()
            cleaned = 0
            
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.pkl'):
                    path = os.path.join(self.cache_dir, filename)
                    try:
                        age_hours = (now - os.path.getmtime(path)) / 3600
                        if age_hours > self.ttl_hours:
                            os.remove(path)
                            cleaned += 1
                    except Exception:
                        pass
            
            return cleaned
    
    def stats(self) -> dict:
        """统计信息"""
        with self.lock:
            files = [f for f in os.listdir(self.cache_dir) if f.endswith('.pkl')]
            total_size = sum(
                os.path.getsize(os.path.join(self.cache_dir, f)) 
                for f in files
            )
            
            return {
                'files': len(files),
                'total_size_mb': total_size / 1024 / 1024,
                'ttl_hours': self.ttl_hours,
            }


class HierarchicalCache:
    """
    分层缓存管理器
    
    实现 L1(内存) → L2(磁盘) → L3(数据库) 三级缓存架构。
    
    使用示例:
    ```python
    cache = HierarchicalCache(db_path='qlib_pro_v16.db')
    
    # 获取K线数据（自动穿透各层缓存）
    df = cache.get_kline('000001', start_date='2024-01-01')
    
    # 查看缓存统计
    print(cache.stats())
    ```
    """
    
    def __init__(
        self,
        db_path: str = None,
        l1_maxsize: int = 200,
        l2_cache_dir: str = None,
        l2_ttl_hours: int = 24,
    ):
        """
        初始化分层缓存
        
        Args:
            db_path: SQLite数据库路径
            l1_maxsize: L1内存缓存大小
            l2_cache_dir: L2磁盘缓存目录
            l2_ttl_hours: L2缓存TTL（小时）
        """
        self.db_path = db_path
        
        # L1: 内存缓存
        self.l1 = LRUCache(maxsize=l1_maxsize)
        
        # L2: 磁盘缓存
        self.l2 = DiskCache(cache_dir=l2_cache_dir, ttl_hours=l2_ttl_hours)
        
        # 统计
        self._stats = {
            'l1_hits': 0,
            'l2_hits': 0,
            'l3_hits': 0,
            'api_fetches': 0,
        }
    
    def _make_key(self, code: str, start_date: str) -> str:
        """生成缓存键"""
        return f"kline:{code}:{start_date}"
    
    def get_kline(
        self,
        code: str,
        start_date: str,
        fetch_func: Callable[[str, str], pd.DataFrame] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取K线数据（分层缓存穿透）
        
        Args:
            code: 股票代码
            start_date: 起始日期
            fetch_func: API获取函数（可选）
            
        Returns:
            K线DataFrame或None
        """
        key = self._make_key(code, start_date)
        
        # L1: 内存缓存
        result = self.l1.get(key)
        if result is not None:
            self._stats['l1_hits'] += 1
            logger.debug(f"L1 hit: {code}")
            return result
        
        # L2: 磁盘缓存
        result = self.l2.get(key)
        if result is not None:
            self._stats['l2_hits'] += 1
            self.l1.set(key, result)  # 回填L1
            logger.debug(f"L2 hit: {code}")
            return result
        
        # L3: 数据库缓存
        result = self._fetch_from_db(code, start_date)
        if result is not None and len(result) > 0:
            self._stats['l3_hits'] += 1
            self.l1.set(key, result)
            self.l2.set(key, result)
            logger.debug(f"L3 hit: {code}")
            return result
        
        # L4: API获取
        if fetch_func is not None:
            try:
                result = fetch_func(code, start_date)
                if result is not None and len(result) > 0:
                    self._stats['api_fetches'] += 1
                    
                    # 存入所有缓存层
                    self.l1.set(key, result)
                    self.l2.set(key, result)
                    self._save_to_db(code, result)
                    
                    logger.debug(f"API fetch: {code}")
                    return result
            except Exception as e:
                logger.error(f"API fetch error for {code}: {e}")
        
        return None
    
    def _fetch_from_db(self, code: str, start_date: str) -> Optional[pd.DataFrame]:
        """从数据库获取"""
        if self.db_path is None:
            return None
        
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            df = pd.read_sql_query(
                """
                SELECT * FROM kline_cache 
                WHERE code=? AND trade_date >= ? 
                ORDER BY trade_date
                """,
                conn,
                params=(code, start_date)
            )
            conn.close()
            return df if len(df) > 0 else None
        except Exception as e:
            logger.warning(f"DB read error: {e}")
            return None
    
    def _save_to_db(self, code: str, df: pd.DataFrame):
        """保存到数据库"""
        if self.db_path is None:
            return
        
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            df_to_save = df.copy()
            df_to_save['code'] = code
            df_to_save.to_sql('kline_cache', conn, if_exists='append', index=False)
            conn.close()
        except Exception as e:
            logger.warning(f"DB write error: {e}")
    
    def invalidate(self, code: str, start_date: str = None):
        """使缓存失效"""
        if start_date:
            key = self._make_key(code, start_date)
            self.l1.delete(key)
            self.l2.delete(key)
        else:
            # 清除该股票所有缓存（需要遍历）
            pass
    
    def clear_all(self):
        """清空所有缓存"""
        self.l1.clear()
        self.l2.clear()
        self._stats = {
            'l1_hits': 0,
            'l2_hits': 0,
            'l3_hits': 0,
            'api_fetches': 0,
        }
    
    def stats(self) -> dict:
        """获取缓存统计"""
        total_requests = sum(self._stats.values())
        
        return {
            'l1': self.l1.stats(),
            'l2': self.l2.stats(),
            'hits': {
                'l1': self._stats['l1_hits'],
                'l2': self._stats['l2_hits'],
                'l3': self._stats['l3_hits'],
                'api': self._stats['api_fetches'],
            },
            'total_requests': total_requests,
            'cache_hit_rate': (
                (self._stats['l1_hits'] + self._stats['l2_hits'] + self._stats['l3_hits'])
                / total_requests if total_requests > 0 else 0.0
            ),
        }
    
    def warmup(self, codes: list, start_date: str, fetch_func: Callable):
        """
        预热缓存
        
        Args:
            codes: 股票代码列表
            start_date: 起始日期
            fetch_func: API获取函数
        """
        logger.info(f"Warming up cache for {len(codes)} stocks...")
        
        for i, code in enumerate(codes):
            try:
                self.get_kline(code, start_date, fetch_func)
                if (i + 1) % 50 == 0:
                    logger.info(f"Warmup progress: {i+1}/{len(codes)}")
            except Exception as e:
                logger.warning(f"Warmup failed for {code}: {e}")
        
        logger.info(f"Cache warmup complete. Stats: {self.stats()}")


# ========== 便捷函数 ==========

_global_cache: Optional[HierarchicalCache] = None


def get_global_cache(db_path: str = None) -> HierarchicalCache:
    """获取全局缓存实例"""
    global _global_cache
    
    if _global_cache is None:
        _global_cache = HierarchicalCache(db_path=db_path)
    
    return _global_cache


def cached_kline(
    code: str,
    start_date: str,
    fetch_func: Callable = None,
    db_path: str = None,
) -> Optional[pd.DataFrame]:
    """
    便捷函数：获取缓存的K线数据
    
    Args:
        code: 股票代码
        start_date: 起始日期
        fetch_func: API获取函数
        db_path: 数据库路径
        
    Returns:
        K线DataFrame
    """
    cache = get_global_cache(db_path)
    return cache.get_kline(code, start_date, fetch_func)
