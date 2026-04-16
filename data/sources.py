# -*- coding: utf-8 -*-
"""
数据源配置模块
统一管理盘中/盘后数据源，避免字段不兼容

=== 数据源规范 ===

| 数据类型           | 盘中数据源        | 盘后数据源        | 字段规范                |
|-------------------|------------------|------------------|------------------------|
| 个股K线           | adata            | adata            | trade_date,open,close,high,low,volume,amount |
| ETF K线           | efinance         | efinance         | trade_date,open,close,high,low,volume,amount |
| 个股实时行情       | Sina             | adata K线        | 股票代码,股票名称,最新价,涨跌幅,成交额,量比 |
| ETF 实时行情       | Sina             | efinance K线     | 股票代码,股票名称,最新价,涨跌幅,成交额,量比 |
| 股票代码列表       | Eastmoney        | adata            | stock_code |
| ETF 列表          | 内置列表          | 内置列表          | 股票代码,股票名称 |
| 板块概念          | adata            | adata            | name |
| 资金流向(大单)     | efinance         | efinance         | 超大单净流入,大单净流入 |
| 美元指数          | efinance         | efinance         | 最新价,涨跌额 |
| 股东人数          | Eastmoney        | Eastmoney        | HOLDER_NUM_RATIO |

=== 交易时段判断 ===
- 盘前: 00:00 - 09:15
- 盘中: 09:15 - 15:00 (竞价 + 连续交易)
- 盘后: 15:00 - 24:00

=== 调用原则 ===
1. 盘中优先使用实时行情 API (Sina/Eastmoney)
2. 盘后回退到 K 线数据 (上一交易日收盘)
3. ETF 统一使用 efinance，个股统一使用 adata
4. 资金流向统一使用 efinance (盘后可能无数据，返回0)
"""

from datetime import datetime, time
from enum import Enum
import logging

logger = logging.getLogger('momentum')


class MarketSession(Enum):
    """交易时段"""
    PRE_MARKET = "pre_market"      # 盘前 (00:00 - 09:15)
    TRADING = "trading"            # 盘中 (09:15 - 15:00)
    POST_MARKET = "post_market"    # 盘后 (15:00 - 24:00)


class DataSource(Enum):
    """数据源类型"""
    ADATA = "adata"           # 个股K线、概念
    EFINANCE = "efinance"     # ETF K线、资金流向
    SINA = "sina"             # 实时行情
    EASTMONEY = "eastmoney"   # 股票代码列表、股东人数
    BUILTIN = "builtin"       # 内置列表


def get_market_session() -> MarketSession:
    """
    判断当前交易时段
    
    Returns:
        MarketSession: 当前时段
    """
    now = datetime.now().time()
    
    # 盘前: 00:00 - 09:15
    if now < time(9, 15):
        return MarketSession.PRE_MARKET
    
    # 盘中: 09:15 - 15:00
    if now < time(15, 0):
        return MarketSession.TRADING
    
    # 盘后: 15:00 - 24:00
    return MarketSession.POST_MARKET


def is_trading_hours() -> bool:
    """判断是否在交易时段 (包括集合竞价)"""
    return get_market_session() == MarketSession.TRADING


def is_trading_day() -> bool:
    """
    判断今天是否为交易日
    简单判断: 周一到周五 (不含节假日)
    
    TODO: 可接入交易日历 API
    """
    weekday = datetime.now().weekday()
    return weekday < 5  # 0-4 是周一到周五


def get_data_source_for(data_type: str, is_realtime: bool = True) -> DataSource:
    """
    根据数据类型和时段获取推荐数据源
    
    Args:
        data_type: 数据类型 (stock_kline, etf_kline, stock_realtime, etf_realtime, 
                   stock_codes, etf_list, concept, big_order, dxy, holder)
        is_realtime: 是否需要实时数据
    
    Returns:
        DataSource: 推荐数据源
    """
    # 静态映射 (不区分盘中盘后)
    static_sources = {
        'stock_kline': DataSource.ADATA,
        'etf_kline': DataSource.EFINANCE,
        'stock_codes': DataSource.EASTMONEY,
        'etf_list': DataSource.BUILTIN,
        'concept': DataSource.ADATA,
        'big_order': DataSource.EFINANCE,
        'dxy': DataSource.EFINANCE,
        'holder': DataSource.EASTMONEY,
    }
    
    if data_type in static_sources:
        return static_sources[data_type]
    
    # 实时行情 (区分盘中盘后)
    session = get_market_session()
    
    if data_type == 'stock_realtime':
        if session == MarketSession.TRADING and is_realtime:
            return DataSource.SINA
        else:
            return DataSource.ADATA  # 盘后用 K 线
    
    if data_type == 'etf_realtime':
        if session == MarketSession.TRADING and is_realtime:
            return DataSource.SINA
        else:
            return DataSource.EFINANCE  # 盘后用 K 线
    
    return DataSource.ADATA  # 默认


# 标准化字段映射
FIELD_MAPPING = {
    # adata K 线字段 (标准)
    'adata_kline': {
        'trade_date': 'trade_date',
        'open': 'open',
        'close': 'close',
        'high': 'high',
        'low': 'low',
        'volume': 'volume',
        'amount': 'amount',
        'turnover_ratio': 'turnover_ratio',
    },
    # efinance K 线字段 -> 标准化
    'efinance_kline': {
        '日期': 'trade_date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '换手率': 'turnover_ratio',
    },
    # Sina 实时行情字段 (标准)
    'sina_realtime': {
        '股票代码': '股票代码',
        '股票名称': '股票名称',
        '最新价': '最新价',
        '涨跌幅': '涨跌幅',
        '涨跌额': '涨跌额',
        '成交量': '成交量',
        '成交额': '成交额',
        '最高': '最高',
        '最低': '最低',
        '今开': '今开',
        '昨日收盘': '昨日收盘',
        '量比': '量比',
        '换手率': '换手率',
        '总市值': '总市值',
    },
}


def normalize_kline_df(df, source: DataSource):
    """
    标准化 K 线 DataFrame 字段名
    
    Args:
        df: 原始 DataFrame
        source: 数据源
    
    Returns:
        标准化后的 DataFrame
    """
    if df is None or df.empty:
        return df
    
    if source == DataSource.EFINANCE:
        mapping = FIELD_MAPPING['efinance_kline']
        df = df.rename(columns=mapping)
    # adata 已经是标准格式，不需要转换
    
    return df


def log_data_source(data_type: str, source: DataSource, session: MarketSession):
    """记录数据源使用日志"""
    logger.debug(f"[DataSource] {data_type} -> {source.value} ({session.value})")
