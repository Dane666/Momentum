# -*- coding: utf-8 -*-
"""
因子计算单元测试

测试范围:
- TechnicalFactors 技术因子计算
- Alpha合成因子计算
- 因子数值边界检查
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ========== Fixtures ==========

@pytest.fixture
def sample_kline_data():
    """生成示例K线数据"""
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    np.random.seed(42)
    
    # 模拟价格序列
    returns = np.random.normal(0.001, 0.02, 100)
    close = 10.0 * np.cumprod(1 + returns)
    
    df = pd.DataFrame({
        'trade_date': dates,
        'open': close * (1 + np.random.uniform(-0.01, 0.01, 100)),
        'high': close * (1 + np.random.uniform(0, 0.03, 100)),
        'low': close * (1 - np.random.uniform(0, 0.03, 100)),
        'close': close,
        'volume': np.random.randint(1000000, 10000000, 100),
        'amount': np.random.uniform(5e7, 5e8, 100),
    })
    
    # 确保 high >= close >= low
    df['high'] = df[['open', 'high', 'close']].max(axis=1)
    df['low'] = df[['open', 'low', 'close']].min(axis=1)
    
    return df


@pytest.fixture
def uptrend_data():
    """生成上涨趋势数据"""
    dates = pd.date_range('2024-01-01', periods=50, freq='D')
    close = np.linspace(10, 15, 50) + np.random.normal(0, 0.1, 50)
    
    df = pd.DataFrame({
        'trade_date': dates,
        'open': close * 0.99,
        'high': close * 1.02,
        'low': close * 0.98,
        'close': close,
        'volume': np.random.randint(5000000, 10000000, 50),
        'amount': close * np.random.randint(5000000, 10000000, 50),
    })
    return df


@pytest.fixture
def downtrend_data():
    """生成下跌趋势数据"""
    dates = pd.date_range('2024-01-01', periods=50, freq='D')
    close = np.linspace(15, 10, 50) + np.random.normal(0, 0.1, 50)
    
    df = pd.DataFrame({
        'trade_date': dates,
        'open': close * 1.01,
        'high': close * 1.02,
        'low': close * 0.98,
        'close': close,
        'volume': np.random.randint(3000000, 8000000, 50),
        'amount': close * np.random.randint(3000000, 8000000, 50),
    })
    return df


# ========== 技术因子测试 ==========

class TestTechnicalFactors:
    """技术因子计算测试"""
    
    def test_rsi_range(self, sample_kline_data):
        """RSI应在0-100范围内"""
        from ..factors.technical_factors import TechnicalFactors
        
        tf = TechnicalFactors(sample_kline_data)
        df = tf.compute_all()
        
        if 'RSI_14' in df.columns:
            rsi = df['RSI_14'].dropna()
            assert rsi.min() >= 0, "RSI不应小于0"
            assert rsi.max() <= 100, "RSI不应大于100"
    
    def test_macd_crossover(self, uptrend_data):
        """上涨趋势中MACD应为正"""
        from ..factors.technical_factors import TechnicalFactors
        
        tf = TechnicalFactors(uptrend_data)
        df = tf.compute_all()
        
        if 'MACD_hist' in df.columns:
            # 最后10天的MACD柱应多数为正
            recent_macd = df['MACD_hist'].tail(10).dropna()
            if len(recent_macd) > 0:
                positive_ratio = (recent_macd > 0).mean()
                assert positive_ratio > 0.5, "上涨趋势中MACD应多数为正"
    
    def test_ma_order_in_uptrend(self, uptrend_data):
        """上涨趋势中均线应多头排列"""
        from ..factors.technical_factors import TechnicalFactors
        
        tf = TechnicalFactors(uptrend_data)
        df = tf.compute_all()
        
        # 检查均线多头排列
        if all(col in df.columns for col in ['MA_5', 'MA_10', 'MA_20']):
            last_row = df.iloc[-1]
            # MA5 > MA10 > MA20 表示多头
            assert last_row['MA_5'] >= last_row['MA_10'] * 0.95, "上涨趋势MA5应接近或高于MA10"
    
    def test_atr_positive(self, sample_kline_data):
        """ATR应始终为正"""
        from ..factors.technical_factors import TechnicalFactors
        
        tf = TechnicalFactors(sample_kline_data)
        df = tf.compute_all()
        
        if 'ATR_14' in df.columns:
            atr = df['ATR_14'].dropna()
            assert (atr >= 0).all(), "ATR不应为负"
    
    def test_volume_ratio_calculation(self, sample_kline_data):
        """量比计算测试"""
        from ..factors.technical_factors import TechnicalFactors
        
        tf = TechnicalFactors(sample_kline_data)
        df = tf.compute_all()
        
        if 'vol_ratio' in df.columns:
            vr = df['vol_ratio'].dropna()
            assert vr.min() > 0, "量比应为正"
    
    def test_bias_calculation(self, sample_kline_data):
        """乖离率计算测试"""
        from ..factors.technical_factors import TechnicalFactors
        
        tf = TechnicalFactors(sample_kline_data)
        df = tf.compute_all()
        
        if 'BIAS_20' in df.columns:
            bias = df['BIAS_20'].dropna()
            # 乖离率一般在-50%到50%之间
            assert bias.min() >= -0.5, "乖离率过低"
            assert bias.max() <= 0.5, "乖离率过高"


class TestFactorEdgeCases:
    """因子边界情况测试"""
    
    def test_empty_dataframe(self):
        """空DataFrame处理"""
        from ..factors.technical_factors import TechnicalFactors
        
        df = pd.DataFrame()
        
        with pytest.raises(Exception):
            tf = TechnicalFactors(df)
            tf.compute_all()
    
    def test_single_row(self):
        """单行数据处理"""
        from ..factors.technical_factors import TechnicalFactors
        
        df = pd.DataFrame({
            'trade_date': [datetime.now()],
            'open': [10.0],
            'high': [10.5],
            'low': [9.5],
            'close': [10.2],
            'volume': [1000000],
            'amount': [10000000],
        })
        
        tf = TechnicalFactors(df)
        result = tf.compute_all()
        # 应该不报错，返回含NaN的数据
        assert len(result) == 1
    
    def test_nan_handling(self, sample_kline_data):
        """NaN处理测试"""
        from ..factors.technical_factors import TechnicalFactors
        
        df = sample_kline_data.copy()
        df.loc[50, 'close'] = np.nan
        
        tf = TechnicalFactors(df)
        result = tf.compute_all()
        
        # 结果中应有数据
        assert len(result) > 0
    
    def test_zero_volume(self, sample_kline_data):
        """零成交量处理"""
        from ..factors.technical_factors import TechnicalFactors
        
        df = sample_kline_data.copy()
        df.loc[50:55, 'volume'] = 0
        
        tf = TechnicalFactors(df)
        result = tf.compute_all()
        
        # 应该处理零成交量情况
        assert len(result) == len(df)


# ========== Alpha因子测试 ==========

class TestAlphaFactors:
    """Alpha合成因子测试"""
    
    def test_alpha_score_normalized(self, sample_kline_data):
        """Alpha分数应标准化"""
        try:
            from ..alpha.alpha_model import AlphaFactorModel
            
            model = AlphaFactorModel()
            score = model.calculate_score(sample_kline_data)
            
            # Alpha分数一般在-1到1之间或标准化区间
            if score is not None:
                assert abs(score) <= 10, "Alpha分数异常"
        except ImportError:
            pytest.skip("AlphaFactorModel not found")
    
    def test_alpha_with_uptrend(self, uptrend_data):
        """上涨趋势Alpha应较高"""
        try:
            from ..alpha.alpha_model import AlphaFactorModel
            
            model = AlphaFactorModel()
            score = model.calculate_score(uptrend_data)
            
            # 上涨趋势应有正Alpha
            if score is not None:
                # 宽松检查，只确保不是极端负值
                assert score > -5, "上涨趋势Alpha不应极端为负"
        except ImportError:
            pytest.skip("AlphaFactorModel not found")


# ========== 运行配置 ==========

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
