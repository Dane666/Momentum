# -*- coding: utf-8 -*-
"""
Momentum Data Module - 数据层
- 数据库操作
- K线缓存
- API数据获取
- 数据源规范管理
- 全局代理禁用
- 回测交易记录
- 交易原因分析
"""

# 首先导入代理防护（确保在任何网络请求前禁用代理）
from .proxy_guard import disable_proxy
disable_proxy()

from .db import (
    init_db,
    get_db_connection,
    save_factor_logs, 
    save_backtest_logs,
    # 回测交易记录器
    BacktestTradeRecorder,
    # 查询函数
    get_backtest_sessions,
    get_session_trades,
    get_session_equity_curve,
    get_session_positions,
    get_trade_statistics,
    delete_session,
    clear_all_backtest_data,
)
from .cache import (
    load_or_fetch_kline, 
    clear_kline_cache, 
    get_cache_stats, 
    reset_cache_stats,
    # 5分钟K线缓存 (14:45精确选股)
    load_or_fetch_5min_kline,
    get_1445_data_from_cache,
    init_5min_kline_table,
)

# 14:45专用数据缓存 (轻量级，只存价格和成交额)
from .cache_1445 import (
    init_1445_cache_table,
    save_1445_data,
    save_1445_data_batch,
    load_1445_data,
    load_1445_data_batch,
    get_cached_dates,
    get_cache_stats as get_1445_cache_stats,
    clear_1445_cache,
)

# 交易原因分析模块
from .trade_reason import (
    TradeReasonAnalyzer,
    TradeReason,
    explain_strategy_rules,
    analyze_trades_from_db,
)
from .fetcher import (
    fetch_kline_from_api,
    fetch_realtime_quotes,
    fetch_stock_concept,
    fetch_market_index,
    fetch_etf_list,
    fetch_etf_quotes_with_fallback,
    fetch_all_stock_codes_local,
    fetch_all_stock_codes_eastmoney,
    fetch_quotes_sina,
    is_etf,
    # 5分钟K线 (14:45精确选股)
    fetch_5min_kline,
    extract_1445_data,
)
from .sources import (
    get_market_session,
    is_trading_hours,
    is_trading_day,
    MarketSession,
    DataSource,
)

__all__ = [
    'init_db',
    'get_db_connection',
    'save_factor_logs',
    'save_backtest_logs',
    # 回测交易记录
    'BacktestTradeRecorder',
    'get_backtest_sessions',
    'get_session_trades',
    'get_session_equity_curve',
    'get_session_positions',
    'get_trade_statistics',
    'delete_session',
    'clear_all_backtest_data',
    # K线缓存
    'load_or_fetch_kline',
    'clear_kline_cache',
    'get_cache_stats',
    'reset_cache_stats',
    # 5分钟K线缓存 (14:45精确选股)
    'load_or_fetch_5min_kline',
    'get_1445_data_from_cache',
    'init_5min_kline_table',
    'fetch_kline_from_api',
    'fetch_realtime_quotes',
    'fetch_stock_concept',
    'fetch_market_index',
    'fetch_etf_list',
    'fetch_etf_quotes_with_fallback',
    'fetch_all_stock_codes_local',
    'fetch_all_stock_codes_eastmoney',
    'fetch_quotes_sina',
    'is_etf',
    # 5分钟K线获取 (14:45精确选股)
    'fetch_5min_kline',
    'extract_1445_data',
    # 数据源管理
    'get_market_session',
    'is_trading_hours',
    'is_trading_day',
    'MarketSession',
    'DataSource',
    # 代理防护
    'disable_proxy',
]

