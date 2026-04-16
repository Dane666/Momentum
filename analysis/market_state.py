# -*- coding: utf-8 -*-
"""
Market State Analysis
Quantify "Trending" vs "Ranging" using ADX and ATR.
"""

import sys
import os
import pandas as pd
import logging

# Add parent directory to path to allow importing momentum modules
# Assuming this script is in tests/momentum/analysis/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from momentum.data.fetcher import fetch_kline_from_api, fetch_market_index
from momentum.data.cache import load_or_fetch_kline
from momentum.factors.technical import compute_adx, compute_atr

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('market_analysis')

def analyze_market_state(code: str, name: str = "", start_date: str = '2024-01-01'):
    """
    Analyze whether the market for a given code is Trending or Ranging.
    
    Args:
        code: Stock or Index code.
        name: Name of the asset.
        start_date: Start date for data fetching.
    """
    logger.info(f"Fetching data for {name} ({code})...")
    
    # Try fetching as index first if code looks like an index, otherwise stock
    # Note: fetch_kline_from_api handles stocks. fetch_market_index handles indices.
    # Simple heuristic: if code starts with '000' and length is 6, could be SH index or stock.
    # But let's just try stock fetcher first for general stocks, and use specific for index if needed.
    
    df = None
    if code in ['000300', '000001', '399001', '399006']:
         df = fetch_market_index(code)
    else:
        # 使用缓存版本，避免重复获取已缓存数据
        df = load_or_fetch_kline(code, fetch_kline_from_api, start_date)
        
    if df is None or df.empty:
        logger.error(f"Failed to fetch data for {code}")
        return

    # Ensure numeric types
    for col in ['high', 'low', 'close', 'open', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df = df.sort_values('trade_date').reset_index(drop=True)
    
    if len(df) < 30:
        logger.warning(f"Not enough data for {code} to calculate indicators.")
        return

    # Calculate Indicators
    adx = compute_adx(df, period=14)
    atr = compute_atr(df, period=14)
    
    # Get latest close price
    current_price = df['close'].iloc[-1]
    
    # Determine State
    # Common ADX interpretation:
    # ADX < 20: Weak trend / Ranging
    # ADX > 25: Strong trend
    # 20-25: Indeterminate
    
    state = "Unknown"
    if adx < 20:
        state = "Ranging (Oscillation)"
    elif adx > 25:
        state = "Trending"
    else:
        state = "Transitioning / Weak Trend"
        
    # ATR relative to price (Volatility %)
    atr_pct = (atr / current_price) * 100
    
    print("-" * 40)
    print(f"Asset: {name} ({code})")
    print(f"Date: {df['trade_date'].iloc[-1]}")
    print(f"Close: {current_price:.2f}")
    print(f"ADX (14): {adx:.2f}")
    print(f"ATR (14): {atr:.2f} ({atr_pct:.2f}%)")
    print(f"Market State: {state}")
    print("-" * 40)
    
    return {
        'code': code,
        'adx': adx,
        'atr': atr,
        'state': state
    }

if __name__ == "__main__":
    # Test with some major indices and stocks
    
    # Note: '000300' fetcher in momentum might be using a mock or simple fetcher.
    # Let's try to use what's available.
    
    targets = [
        ('000300', 'CSI 300'),
        ('399006', 'ChiNext Index'),
        ('600519', 'Kweichow Moutai'), # Stable large cap
        ('300750', 'CATL'),            # Volatile large cap
    ]
    
    print("\nStarting Market State Analysis...\n")
    
    for code, name in targets:
        analyze_market_state(code, name, start_date='2024-06-01')
