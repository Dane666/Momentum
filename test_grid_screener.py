#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网格交易筛选器 - 快速测试脚本

快速验证网格交易筛选功能，使用预设的测试数据
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from strategies.grid_trading_screener import GridTradingScreener


def generate_test_data(pattern='sideways', days=500):
    """
    生成测试数据
    
    Args:
        pattern: 'sideways'(横盘), 'uptrend'(上涨), 'downtrend'(下跌), 'volatile'(剧烈波动)
        days: 天数
    """
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    
    # 基础价格
    base_price = 10.0
    
    if pattern == 'sideways':
        # 横盘震荡 - 适合网格
        trend = np.zeros(days)
        noise = np.random.randn(days) * 0.3
        prices = base_price + np.cumsum(noise)
        # 限制在区间内
        prices = np.clip(prices, base_price * 0.85, base_price * 1.15)
        
    elif pattern == 'uptrend':
        # 上涨趋势 - 不适合网格
        trend = np.linspace(0, 5, days)
        noise = np.random.randn(days) * 0.2
        prices = base_price + trend + np.cumsum(noise)
        
    elif pattern == 'downtrend':
        # 下跌趋势 - 不适合网格
        trend = np.linspace(0, -5, days)
        noise = np.random.randn(days) * 0.2
        prices = base_price + trend + np.cumsum(noise)
        
    else:  # volatile
        # 剧烈波动 - 风险太高
        noise = np.random.randn(days) * 0.8
        prices = base_price + np.cumsum(noise)
    
    # 生成OHLC数据
    df = pd.DataFrame({
        'date': dates,
        'open': prices * (1 + np.random.randn(days) * 0.005),
        'high': prices * (1 + np.abs(np.random.randn(days)) * 0.01),
        'low': prices * (1 - np.abs(np.random.randn(days)) * 0.01),
        'close': prices,
        'volume': np.random.randint(1000000, 5000000, days)
    })
    
    # 确保 high >= close >= low
    df['high'] = df[['open', 'high', 'close']].max(axis=1)
    df['low'] = df[['open', 'low', 'close']].min(axis=1)
    
    return df


def test_screener():
    """测试筛选器功能"""
    print("=" * 80)
    print("网格交易筛选器 - 功能测试")
    print("=" * 80)
    
    screener = GridTradingScreener(lookback_years=3)
    
    # 测试不同模式的数据
    test_cases = [
        ('TEST001', '横盘震荡股', 'sideways'),
        ('TEST002', '上涨趋势股', 'uptrend'),
        ('TEST003', '下跌趋势股', 'downtrend'),
        ('TEST004', '剧烈波动股', 'volatile'),
    ]
    
    results = []
    
    for code, name, pattern in test_cases:
        print(f"\n测试 {code} - {name} ({pattern})...")
        
        # 生成测试数据
        df = generate_test_data(pattern=pattern, days=500)
        
        # 筛选
        result = screener.screen_single_stock(code, name, df)
        
        if result:
            results.append(result)
            print(f"  ✓ 通过筛选")
            print(f"    评分: {result['grid_score']:.1f}")
            print(f"    波动率: {result['volatility']:.2f}%")
            print(f"    趋势强度: {result['trend_strength']:.3f}")
        else:
            print(f"  × 未通过筛选 (可能是流动性不足或评分过低)")
            
            # 仍然计算指标用于展示
            try:
                vol = screener.calculate_volatility(df['close'])
                trend = screener.calculate_trend_strength(df['close'].tail(120))
                print(f"    波动率: {vol:.2f}%")
                print(f"    趋势强度: {trend:.3f}")
            except:
                pass
    
    # 显示结果
    if results:
        print("\n" + "=" * 80)
        print("通过筛选的标的")
        print("=" * 80)
        df_result = screener.format_screening_result(results)
        print(df_result.to_string(index=False))
    else:
        print("\n没有标的通过筛选（这是正常的，因为测试数据可能不满足所有条件）")
    
    print("\n" + "=" * 80)
    print("功能测试完成")
    print("=" * 80)
    print("\n说明:")
    print("- 横盘震荡股应该得分较高（适合网格）")
    print("- 趋势股应该得分较低（趋势强度高）")
    print("- 剧烈波动股虽然波动率高，但风险也大")
    print("\n下一步: 使用真实数据进行筛选")
    print("  python main.py --mode grid --target-type etf")


def demonstrate_indicators():
    """演示各项指标的计算"""
    print("\n" + "=" * 80)
    print("指标计算演示")
    print("=" * 80)
    
    screener = GridTradingScreener()
    
    # 生成横盘数据
    df = generate_test_data(pattern='sideways', days=120)
    
    print("\n1. 波动率计算")
    vol = screener.calculate_volatility(df['close'])
    print(f"   年化波动率: {vol:.2f}%")
    print(f"   评价: ", end='')
    if 15 <= vol <= 40:
        print("✓ 适中，适合网格交易")
    elif vol < 15:
        print("× 波动太小，收益有限")
    else:
        print("× 波动太大，风险较高")
    
    print("\n2. 趋势强度计算")
    trend = screener.calculate_trend_strength(df['close'])
    print(f"   R²值: {trend:.3f}")
    print(f"   评价: ", end='')
    if trend < 0.3:
        print("✓ 横盘震荡，非常适合网格")
    elif trend < 0.5:
        print("○ 弱趋势，可以网格")
    else:
        print("× 强趋势，不适合网格")
    
    print("\n3. 价格区间分析")
    range_metrics = screener.calculate_price_range_stability(df)
    print(f"   区间幅度: {range_metrics['range_ratio']:.2f}%")
    print(f"   当前位置: {range_metrics['position_in_range']:.2f}")
    print(f"   支撑位: {range_metrics['support']:.2f}")
    print(f"   阻力位: {range_metrics['resistance']:.2f}")
    
    position = range_metrics['position_in_range']
    if position <= 0.3:
        print(f"   评价: ✓ 在底部区域，建仓时机好")
    elif position >= 0.7:
        print(f"   评价: ⚠ 在顶部区域，谨慎建仓")
    else:
        print(f"   评价: ○ 在中部区域")
    
    print("\n4. 流动性检查")
    liquidity = screener.calculate_liquidity(df)
    print(f"   日均成交额: {liquidity['avg_turnover']:,.0f} 万元")
    print(f"   评价: ", end='')
    if liquidity['is_liquid']:
        print("✓ 流动性充足")
    else:
        print("× 流动性不足")


if __name__ == '__main__':
    print(__doc__)
    
    # 运行测试
    test_screener()
    
    # 演示指标
    demonstrate_indicators()
    
    print("\n" + "=" * 80)
    print("完整使用指南请查看:")
    print("  docs/GRID_TRADING_GUIDE.md")
    print("=" * 80)
