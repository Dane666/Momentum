# -*- coding: utf-8 -*-
"""
回测指标计算单元测试

测试范围:
- 收益率计算
- 夏普比率计算
- 最大回撤计算
- 胜率计算
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ========== Fixtures ==========

@pytest.fixture
def sample_trades():
    """生成示例交易记录"""
    return pd.DataFrame({
        'entry_date': pd.date_range('2024-01-01', periods=20, freq='5D'),
        'exit_date': pd.date_range('2024-01-03', periods=20, freq='5D'),
        'stock_code': [f'00000{i}' for i in range(20)],
        'entry_price': [10.0] * 20,
        'exit_price': [10.5, 9.8, 11.0, 10.2, 9.5, 10.8, 10.3, 9.9, 10.7, 10.1,
                       10.6, 9.7, 10.9, 10.0, 9.6, 10.4, 10.2, 10.8, 9.8, 10.5],
        'return_pct': [0.05, -0.02, 0.10, 0.02, -0.05, 0.08, 0.03, -0.01, 0.07, 0.01,
                       0.06, -0.03, 0.09, 0.0, -0.04, 0.04, 0.02, 0.08, -0.02, 0.05],
    })


@pytest.fixture
def positive_trades():
    """全正收益交易"""
    return pd.DataFrame({
        'entry_date': pd.date_range('2024-01-01', periods=10, freq='D'),
        'exit_date': pd.date_range('2024-01-02', periods=10, freq='D'),
        'stock_code': [f'00000{i}' for i in range(10)],
        'entry_price': [10.0] * 10,
        'exit_price': [10.5, 10.3, 10.8, 10.2, 10.6, 10.4, 10.7, 10.3, 10.5, 10.9],
        'return_pct': [0.05, 0.03, 0.08, 0.02, 0.06, 0.04, 0.07, 0.03, 0.05, 0.09],
    })


@pytest.fixture
def equity_curve():
    """生成权益曲线"""
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    np.random.seed(42)
    
    # 模拟权益曲线
    returns = np.random.normal(0.001, 0.015, 100)
    equity = 1000000 * np.cumprod(1 + returns)
    
    return pd.Series(equity, index=dates)


# ========== 收益率计算测试 ==========

class TestReturnCalculations:
    """收益率计算测试"""
    
    def test_total_return_positive(self, positive_trades):
        """正收益交易的总收益应为正"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(positive_trades)
        metrics = mc.calculate_all()
        
        assert metrics.total_return > 0, "全正交易的总收益应为正"
    
    def test_return_matches_trades(self, sample_trades):
        """总收益应匹配交易记录"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(sample_trades)
        metrics = mc.calculate_all()
        
        # 计算期望收益
        expected_return = (1 + sample_trades['return_pct']).prod() - 1
        
        # 允许5%误差
        assert abs(metrics.total_return - expected_return) < 0.05, "总收益计算不匹配"
    
    def test_annual_return_calculation(self, sample_trades):
        """年化收益率计算"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(sample_trades)
        metrics = mc.calculate_all()
        
        # 年化收益应在合理范围内
        assert -1.0 <= metrics.annual_return <= 10.0, "年化收益超出合理范围"


# ========== 夏普比率测试 ==========

class TestSharpeRatio:
    """夏普比率测试"""
    
    def test_sharpe_calculation(self, sample_trades):
        """夏普比率基本计算"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(sample_trades)
        metrics = mc.calculate_all()
        
        # 夏普应在合理范围内
        assert -5 <= metrics.sharpe <= 10, "夏普比率超出合理范围"
    
    def test_high_sharpe_for_consistent_returns(self, positive_trades):
        """稳定正收益应有较高夏普"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(positive_trades)
        metrics = mc.calculate_all()
        
        # 全正收益交易应有较高夏普
        assert metrics.sharpe > 0, "全正收益的夏普应为正"
    
    def test_sharpe_with_zero_std(self):
        """零波动率情况处理"""
        from ..backtest.metrics import MetricsCalculator
        
        # 所有交易收益相同
        trades = pd.DataFrame({
            'entry_date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'exit_date': pd.date_range('2024-01-02', periods=5, freq='D'),
            'stock_code': ['000001'] * 5,
            'entry_price': [10.0] * 5,
            'exit_price': [10.5] * 5,
            'return_pct': [0.05] * 5,
        })
        
        mc = MetricsCalculator(trades)
        # 应该不会崩溃
        metrics = mc.calculate_all()


# ========== 最大回撤测试 ==========

class TestMaxDrawdown:
    """最大回撤测试"""
    
    def test_drawdown_range(self, equity_curve):
        """最大回撤应在0-100%之间"""
        from ..backtest.metrics import MetricsCalculator
        
        # 从权益曲线计算回撤
        peak = equity_curve.cummax()
        drawdown = (equity_curve - peak) / peak
        max_dd = abs(drawdown.min())
        
        assert 0 <= max_dd <= 1, "回撤应在0-100%之间"
    
    def test_no_drawdown_for_monotonic(self):
        """单调上涨无回撤"""
        equity = pd.Series([100, 110, 120, 130, 140, 150])
        
        peak = equity.cummax()
        drawdown = (equity - peak) / peak
        max_dd = abs(drawdown.min())
        
        assert max_dd == 0, "单调上涨应无回撤"
    
    def test_drawdown_calculation(self):
        """回撤计算准确性"""
        # 从100涨到120再跌到90
        equity = pd.Series([100, 110, 120, 100, 90, 95])
        
        peak = equity.cummax()
        drawdown = (equity - peak) / peak
        max_dd = abs(drawdown.min())
        
        expected = (120 - 90) / 120  # 25%
        assert abs(max_dd - expected) < 0.001, "回撤计算不准确"


# ========== 胜率测试 ==========

class TestWinRate:
    """胜率测试"""
    
    def test_win_rate_range(self, sample_trades):
        """胜率应在0-100%之间"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(sample_trades)
        metrics = mc.calculate_all()
        
        assert 0 <= metrics.win_rate <= 1, "胜率应在0-100%之间"
    
    def test_100_percent_win_rate(self, positive_trades):
        """全赢交易胜率应为100%"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(positive_trades)
        metrics = mc.calculate_all()
        
        assert metrics.win_rate == 1.0, "全正收益胜率应为100%"
    
    def test_win_rate_calculation(self, sample_trades):
        """胜率计算准确性"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(sample_trades)
        metrics = mc.calculate_all()
        
        # 手动计算胜率
        expected_win_rate = (sample_trades['return_pct'] > 0).mean()
        
        assert abs(metrics.win_rate - expected_win_rate) < 0.01, "胜率计算不准确"


# ========== 综合指标测试 ==========

class TestMetricsConsistency:
    """指标一致性测试"""
    
    def test_metrics_relationship(self, sample_trades):
        """指标间关系验证"""
        from ..backtest.metrics import MetricsCalculator
        
        mc = MetricsCalculator(sample_trades)
        metrics = mc.calculate_all()
        
        # 正收益应该有非负总收益
        if metrics.win_rate > 0.5 and metrics.profit_factor > 1:
            assert metrics.total_return >= 0, "高胜率+正盈亏比应有正收益"
    
    def test_empty_trades(self):
        """空交易记录处理"""
        from ..backtest.metrics import MetricsCalculator
        
        empty_trades = pd.DataFrame(columns=[
            'entry_date', 'exit_date', 'stock_code', 
            'entry_price', 'exit_price', 'return_pct'
        ])
        
        mc = MetricsCalculator(empty_trades)
        # 应该返回默认值或安全处理
        try:
            metrics = mc.calculate_all()
        except Exception:
            pass  # 可以抛出异常


# ========== 运行配置 ==========

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
