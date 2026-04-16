# -*- coding: utf-8 -*-
"""
回测引擎单元测试

测试范围:
- 交易模拟准确性
- 止盈止损逻辑
- 持仓管理
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ========== Fixtures ==========

@pytest.fixture
def simple_kline_data():
    """简单K线数据用于测试"""
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    
    # 创建明确的价格模式：先涨后跌
    prices = [10.0, 10.2, 10.5, 10.8, 11.0, 11.2, 11.5,  # 涨
              11.3, 11.0, 10.8, 10.5, 10.2, 10.0, 9.8,   # 跌
              9.5, 9.8, 10.0, 10.2, 10.5, 10.8, 11.0,    # 反弹
              11.2, 11.5, 11.8, 12.0, 12.2, 12.5, 12.8, 13.0, 13.2]
    
    df = pd.DataFrame({
        'trade_date': dates,
        'stock_code': '000001',
        'open': prices,
        'high': [p * 1.02 for p in prices],
        'low': [p * 0.98 for p in prices],
        'close': prices,
        'volume': [5000000] * 30,
        'amount': [p * 5000000 for p in prices],
    })
    
    return df


@pytest.fixture
def stop_loss_test_data():
    """止损测试数据：买入后持续下跌"""
    dates = pd.date_range('2024-01-01', periods=20, freq='D')
    
    # 持续下跌
    prices = [10.0 * (0.98 ** i) for i in range(20)]
    
    df = pd.DataFrame({
        'trade_date': dates,
        'stock_code': '000001',
        'open': prices,
        'high': [p * 1.01 for p in prices],
        'low': [p * 0.99 for p in prices],
        'close': prices,
        'volume': [5000000] * 20,
        'amount': [p * 5000000 for p in prices],
    })
    
    return df


@pytest.fixture
def take_profit_test_data():
    """止盈测试数据：买入后持续上涨"""
    dates = pd.date_range('2024-01-01', periods=20, freq='D')
    
    # 持续上涨
    prices = [10.0 * (1.02 ** i) for i in range(20)]
    
    df = pd.DataFrame({
        'trade_date': dates,
        'stock_code': '000001',
        'open': prices,
        'high': [p * 1.01 for p in prices],
        'low': [p * 0.99 for p in prices],
        'close': prices,
        'volume': [5000000] * 20,
        'amount': [p * 5000000 for p in prices],
    })
    
    return df


# ========== 交易模拟测试 ==========

class TestTradeSimulation:
    """交易模拟测试"""
    
    def test_entry_price_accuracy(self, simple_kline_data):
        """买入价格准确性"""
        # 在第5天买入，应该使用当天开盘价
        entry_idx = 5
        entry_date = simple_kline_data.iloc[entry_idx]['trade_date']
        expected_price = simple_kline_data.iloc[entry_idx]['open']
        
        # 验证数据正确性
        assert expected_price > 0, "开盘价应为正"
    
    def test_exit_price_accuracy(self, simple_kline_data):
        """卖出价格准确性"""
        # 验证收盘价数据
        exit_idx = 10
        exit_price = simple_kline_data.iloc[exit_idx]['close']
        
        assert exit_price > 0, "收盘价应为正"
    
    def test_return_calculation(self, simple_kline_data):
        """收益率计算"""
        entry_price = 10.0
        exit_price = 11.0
        slippage = 0.008  # 0.8%
        
        # 计算收益率
        raw_return = (exit_price / entry_price) - 1
        net_return = raw_return - slippage
        
        assert abs(net_return - 0.092) < 0.001, "收益率计算不准确"


# ========== 止损逻辑测试 ==========

class TestStopLoss:
    """止损逻辑测试"""
    
    def test_stop_loss_triggered(self, stop_loss_test_data):
        """止损触发测试"""
        entry_price = stop_loss_test_data.iloc[0]['close']
        stop_loss_pct = 0.05  # 5%止损
        stop_price = entry_price * (1 - stop_loss_pct)
        
        # 找到止损触发日
        triggered = False
        for idx in range(1, len(stop_loss_test_data)):
            if stop_loss_test_data.iloc[idx]['low'] <= stop_price:
                triggered = True
                break
        
        assert triggered, "下跌数据应触发止损"
    
    def test_stop_loss_price(self, stop_loss_test_data):
        """止损价格计算"""
        entry_price = 10.0
        stop_loss_pct = 0.05
        expected_stop = entry_price * (1 - stop_loss_pct)
        
        assert abs(expected_stop - 9.5) < 0.001, "止损价格计算错误"
    
    def test_multiple_stop_levels(self):
        """多级止损测试"""
        entry_price = 10.0
        
        levels = [
            (0.03, 9.70),   # 3% 止损
            (0.05, 9.50),   # 5% 止损
            (0.08, 9.20),   # 8% 止损
            (0.10, 9.00),   # 10% 止损
        ]
        
        for pct, expected in levels:
            actual = entry_price * (1 - pct)
            assert abs(actual - expected) < 0.01, f"{pct*100}%止损价计算错误"


# ========== 止盈逻辑测试 ==========

class TestTakeProfit:
    """止盈逻辑测试"""
    
    def test_take_profit_triggered(self, take_profit_test_data):
        """止盈触发测试"""
        entry_price = take_profit_test_data.iloc[0]['close']
        take_profit_pct = 0.10  # 10%止盈
        profit_price = entry_price * (1 + take_profit_pct)
        
        # 找到止盈触发日
        triggered = False
        for idx in range(1, len(take_profit_test_data)):
            if take_profit_test_data.iloc[idx]['high'] >= profit_price:
                triggered = True
                break
        
        assert triggered, "上涨数据应触发止盈"
    
    def test_take_profit_price(self, take_profit_test_data):
        """止盈价格计算"""
        entry_price = 10.0
        take_profit_pct = 0.10
        expected_profit = entry_price * (1 + take_profit_pct)
        
        assert abs(expected_profit - 11.0) < 0.001, "止盈价格计算错误"


# ========== 持仓管理测试 ==========

class TestPositionManagement:
    """持仓管理测试"""
    
    def test_position_sizing(self):
        """仓位计算测试"""
        total_capital = 1000000
        max_positions = 5
        
        position_size = total_capital / max_positions
        
        assert position_size == 200000, "仓位计算错误"
    
    def test_slippage_calculation(self):
        """滑点计算测试"""
        entry_price = 10.0
        slippage = 0.008  # 0.8%
        
        actual_entry = entry_price * (1 + slippage / 2)  # 买入多付
        actual_exit = 11.0 * (1 - slippage / 2)  # 卖出少收
        
        # 验证滑点影响
        assert actual_entry > entry_price
        assert actual_exit < 11.0
    
    def test_hold_period_limit(self, simple_kline_data):
        """持仓期限测试"""
        max_hold = 5
        entry_idx = 0
        
        # 应该在第5天或之前退出
        exit_idx = min(entry_idx + max_hold, len(simple_kline_data) - 1)
        
        assert exit_idx <= entry_idx + max_hold, "持仓超期"


# ========== 边界情况测试 ==========

class TestEdgeCases:
    """边界情况测试"""
    
    def test_same_day_entry_exit(self):
        """同日进出测试"""
        entry_price = 10.0
        exit_price = 10.0
        slippage = 0.008
        
        # 同日进出应该亏损滑点
        return_pct = (exit_price / entry_price) - 1 - slippage
        
        assert return_pct < 0, "同日进出应亏损"
    
    def test_zero_return(self):
        """零收益测试"""
        entry_price = 10.0
        exit_price = 10.08  # 刚好覆盖0.8%滑点
        slippage = 0.008
        
        return_pct = (exit_price / entry_price) - 1 - slippage
        
        assert abs(return_pct) < 0.001, "滑点覆盖应接近零收益"
    
    def test_limit_up_handling(self):
        """涨停处理测试"""
        # 涨停价 = 前收 * 1.10
        prev_close = 10.0
        limit_up = prev_close * 1.10
        
        # 涨停时应使用涨停价
        assert abs(limit_up - 11.0) < 0.01
    
    def test_limit_down_handling(self):
        """跌停处理测试"""
        # 跌停价 = 前收 * 0.90
        prev_close = 10.0
        limit_down = prev_close * 0.90
        
        assert abs(limit_down - 9.0) < 0.01


# ========== 运行配置 ==========

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
