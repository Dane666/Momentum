"""
量化因子计算模块
================
用于识别庄股和高风险标的的量化因子

因子列表:
1. Momentum Quality (Information Discreteness) - 动量质量/信息离散度
2. Idiosyncratic Volatility (IVOL) - 特质波动率
3. Amihud Illiquidity - 非流动性因子
4. Overnight vs Intraday Momentum - 隔夜与日内动量

Author: Momentum Strategy Team
Date: 2026-02-03
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Tuple, Optional
import warnings

warnings.filterwarnings('ignore')


class QuantFactors:
    """
    量化因子计算器
    
    用于计算识别庄股和高风险标的的量化因子
    """
    
    def __init__(self, df: pd.DataFrame, window: int = 20):
        """
        初始化因子计算器
        
        Parameters:
        -----------
        df : pd.DataFrame
            输入数据，索引为日期，必须包含列:
            ['Open', 'High', 'Low', 'Close', 'Volume', 'Benchmark_Close']
        window : int
            滚动窗口大小，默认20天
        """
        self.df = df.copy()
        self.window = window
        self._validate_columns()
        self._precompute()
    
    def _validate_columns(self):
        """验证必需的列是否存在"""
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing = [col for col in required_cols if col not in self.df.columns]
        if missing:
            raise ValueError(f"缺少必需列: {missing}")
        
        # Benchmark_Close 可选，如果不存在则用 Close 模拟
        if 'Benchmark_Close' not in self.df.columns:
            print("⚠️ 未找到 Benchmark_Close，将使用 Close 的滚动均值模拟")
            self.df['Benchmark_Close'] = self.df['Close'].rolling(20).mean()
    
    def _precompute(self):
        """预计算常用数据"""
        # 对数收盘价
        self.df['Log_Close'] = np.log(self.df['Close'].replace(0, np.nan))
        
        # 个股收益率 (对数收益率)
        self.df['Return'] = self.df['Log_Close'].diff()
        
        # 大盘收益率
        self.df['Benchmark_Return'] = np.log(
            self.df['Benchmark_Close'].replace(0, np.nan)
        ).diff()
        
        # 成交金额
        self.df['Turnover'] = self.df['Close'] * self.df['Volume']
        
        # 隔夜收益率: ln(Open_t / Close_{t-1})
        self.df['Overnight_Return'] = np.log(
            self.df['Open'] / self.df['Close'].shift(1)
        )
        
        # 日内收益率: ln(Close_t / Open_t)
        self.df['Intraday_Return'] = np.log(
            self.df['Close'] / self.df['Open']
        )
    
    # =========================================================================
    # 因子1: Momentum Quality (Information Discreteness)
    # =========================================================================
    def calc_momentum_quality(self, window: Optional[int] = None) -> pd.Series:
        """
        计算动量质量因子 (信息离散度)
        
        逻辑: 对过去N天的 Log(Close) 对 时间(1,2,...,N) 做线性回归，
              输出 R² 值。R² 越高，说明价格走势越"纯净"，动量质量越高。
              庄股通常表现为 R² 很高（走势过于光滑，人为控制痕迹明显）
        
        Parameters:
        -----------
        window : int, optional
            滚动窗口大小，默认使用初始化时的 window
            
        Returns:
        --------
        pd.Series : R² 序列
        """
        w = window or self.window
        log_close = self.df['Log_Close']
        
        def _calc_r_squared(y: np.ndarray) -> float:
            """计算线性回归的 R²"""
            if len(y) < w or np.isnan(y).any():
                return np.nan
            
            x = np.arange(len(y))
            try:
                slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
                return r_value ** 2
            except:
                return np.nan
        
        # 使用 rolling apply 计算
        r_squared = log_close.rolling(window=w, min_periods=w).apply(
            _calc_r_squared, raw=True
        )
        
        return r_squared.rename('Momentum_Quality_R2')
    
    # =========================================================================
    # 因子2: Idiosyncratic Volatility (IVOL)
    # =========================================================================
    def calc_ivol(self, window: Optional[int] = None) -> pd.Series:
        """
        计算特质波动率 (IVOL)
        
        逻辑: 基于 CAPM 模型 r_stock = α + β * r_market + ε
              IVOL = std(ε)，即残差的标准差
              特质波动率高的股票通常风险较高，或存在信息不对称
        
        Parameters:
        -----------
        window : int, optional
            滚动窗口大小
            
        Returns:
        --------
        pd.Series : IVOL 序列
        """
        w = window or self.window
        
        stock_ret = self.df['Return'].values
        market_ret = self.df['Benchmark_Return'].values
        
        def _calc_ivol(idx: int) -> float:
            """计算单个窗口的 IVOL"""
            if idx < w - 1:
                return np.nan
            
            start = idx - w + 1
            y = stock_ret[start:idx+1]
            x = market_ret[start:idx+1]
            
            # 检查 NaN
            mask = ~(np.isnan(y) | np.isnan(x))
            if mask.sum() < w // 2:  # 至少需要一半有效数据
                return np.nan
            
            y, x = y[mask], x[mask]
            
            try:
                # 线性回归
                slope, intercept, _, _, _ = stats.linregress(x, y)
                # 计算残差
                residuals = y - (intercept + slope * x)
                # 残差标准差
                return np.std(residuals, ddof=1)
            except:
                return np.nan
        
        # 向量化计算
        ivol = pd.Series(
            [_calc_ivol(i) for i in range(len(self.df))],
            index=self.df.index,
            name='IVOL'
        )
        
        return ivol
    
    # =========================================================================
    # 因子3: Amihud Illiquidity
    # =========================================================================
    def calc_amihud_illiquidity(self, window: Optional[int] = None) -> pd.Series:
        """
        计算 Amihud 非流动性因子
        
        逻辑: ILLIQ = mean( |Return| / Turnover )
              非流动性越高，表示单位成交金额造成的价格影响越大
              庄股通常流动性差，ILLIQ 值较高
        
        Parameters:
        -----------
        window : int, optional
            滚动窗口大小
            
        Returns:
        --------
        pd.Series : Amihud Illiquidity 序列
        """
        w = window or self.window
        
        # 计算 |Return| / Turnover
        abs_return = self.df['Return'].abs()
        turnover = self.df['Turnover'].replace(0, np.nan)  # 避免除零
        
        # 价格影响比率 (乘以 1e6 便于阅读)
        price_impact = (abs_return / turnover) * 1e6
        
        # 滚动平均
        illiq = price_impact.rolling(window=w, min_periods=w//2).mean()
        
        return illiq.rename('Amihud_Illiquidity')
    
    # =========================================================================
    # 因子4: Overnight vs Intraday Momentum
    # =========================================================================
    def calc_overnight_intraday_momentum(
        self, window: Optional[int] = None
    ) -> Tuple[pd.Series, pd.Series]:
        """
        计算隔夜与日内动量
        
        逻辑: 
        - 隔夜收益: 前一天收盘到今天开盘的收益 = ln(Open_t / Close_{t-1})
        - 日内收益: 今天开盘到今天收盘的收益 = ln(Close_t / Open_t)
        
        异常信号:
        - 隔夜收益占比过高: 可能存在内幕交易或庄家控盘
        - 日内收益与隔夜收益方向持续相反: 可能存在对倒行为
        
        Parameters:
        -----------
        window : int, optional
            滚动窗口大小
            
        Returns:
        --------
        Tuple[pd.Series, pd.Series] : (累积隔夜收益, 累积日内收益)
        """
        w = window or self.window
        
        # 过去N天累积隔夜收益 (对数收益可直接累加)
        cum_overnight = self.df['Overnight_Return'].rolling(
            window=w, min_periods=w//2
        ).sum()
        
        # 过去N天累积日内收益
        cum_intraday = self.df['Intraday_Return'].rolling(
            window=w, min_periods=w//2
        ).sum()
        
        return (
            cum_overnight.rename('Cum_Overnight_Return'),
            cum_intraday.rename('Cum_Intraday_Return')
        )
    
    def calc_overnight_ratio(self, window: Optional[int] = None) -> pd.Series:
        """
        计算隔夜收益占比
        
        隔夜占比 = |累积隔夜收益| / (|累积隔夜收益| + |累积日内收益|)
        
        Parameters:
        -----------
        window : int, optional
            滚动窗口大小
            
        Returns:
        --------
        pd.Series : 隔夜收益占比 (0-1)
        """
        cum_overnight, cum_intraday = self.calc_overnight_intraday_momentum(window)
        
        abs_overnight = cum_overnight.abs()
        abs_intraday = cum_intraday.abs()
        total = abs_overnight + abs_intraday
        
        # 避免除零
        ratio = abs_overnight / total.replace(0, np.nan)
        
        return ratio.rename('Overnight_Ratio')
    
    # =========================================================================
    # 综合计算
    # =========================================================================
    def calc_all_factors(self, window: Optional[int] = None) -> pd.DataFrame:
        """
        计算所有因子
        
        Parameters:
        -----------
        window : int, optional
            滚动窗口大小
            
        Returns:
        --------
        pd.DataFrame : 包含所有因子的 DataFrame
        """
        w = window or self.window
        
        factors = pd.DataFrame(index=self.df.index)
        
        # 1. 动量质量
        factors['Momentum_Quality_R2'] = self.calc_momentum_quality(w)
        
        # 2. 特质波动率
        factors['IVOL'] = self.calc_ivol(w)
        
        # 3. 非流动性
        factors['Amihud_Illiquidity'] = self.calc_amihud_illiquidity(w)
        
        # 4. 隔夜/日内动量
        cum_overnight, cum_intraday = self.calc_overnight_intraday_momentum(w)
        factors['Cum_Overnight_Return'] = cum_overnight
        factors['Cum_Intraday_Return'] = cum_intraday
        factors['Overnight_Ratio'] = self.calc_overnight_ratio(w)
        
        return factors
    
    # =========================================================================
    # 庄股识别评分
    # =========================================================================
    def calc_manipulation_score(
        self, 
        window: Optional[int] = None,
        r2_threshold: float = 0.85,
        ivol_percentile: float = 0.9,
        illiq_percentile: float = 0.9,
        overnight_ratio_threshold: float = 0.7
    ) -> pd.Series:
        """
        计算综合庄股识别评分
        
        评分逻辑 (0-100分):
        - R² > threshold: +25分 (走势过于光滑)
        - IVOL > percentile: +25分 (特质波动过高)
        - Illiquidity > percentile: +25分 (流动性差)
        - Overnight_Ratio > threshold: +25分 (隔夜占比过高)
        
        Parameters:
        -----------
        window : int, optional
            滚动窗口
        r2_threshold : float
            R² 阈值，超过则加分
        ivol_percentile : float
            IVOL 百分位阈值
        illiq_percentile : float
            非流动性百分位阈值
        overnight_ratio_threshold : float
            隔夜占比阈值
            
        Returns:
        --------
        pd.Series : 庄股评分 (0-100)
        """
        factors = self.calc_all_factors(window)
        
        score = pd.Series(0.0, index=factors.index)
        
        # R² 评分
        r2 = factors['Momentum_Quality_R2']
        score += (r2 > r2_threshold).astype(float) * 25
        
        # IVOL 评分 (相对排名)
        ivol = factors['IVOL']
        ivol_thresh = ivol.quantile(ivol_percentile)
        score += (ivol > ivol_thresh).astype(float) * 25
        
        # Illiquidity 评分
        illiq = factors['Amihud_Illiquidity']
        illiq_thresh = illiq.quantile(illiq_percentile)
        score += (illiq > illiq_thresh).astype(float) * 25
        
        # Overnight Ratio 评分
        overnight = factors['Overnight_Ratio']
        score += (overnight > overnight_ratio_threshold).astype(float) * 25
        
        return score.rename('Manipulation_Score')


# =============================================================================
# 便捷函数
# =============================================================================
def calc_momentum_quality(
    df: pd.DataFrame, window: int = 20
) -> pd.Series:
    """快捷计算动量质量因子"""
    return QuantFactors(df, window).calc_momentum_quality()


def calc_ivol(
    df: pd.DataFrame, window: int = 20
) -> pd.Series:
    """快捷计算特质波动率"""
    return QuantFactors(df, window).calc_ivol()


def calc_amihud_illiquidity(
    df: pd.DataFrame, window: int = 20
) -> pd.Series:
    """快捷计算非流动性因子"""
    return QuantFactors(df, window).calc_amihud_illiquidity()


def calc_overnight_intraday(
    df: pd.DataFrame, window: int = 20
) -> Tuple[pd.Series, pd.Series]:
    """快捷计算隔夜/日内动量"""
    return QuantFactors(df, window).calc_overnight_intraday_momentum()


# =============================================================================
# Demo 演示
# =============================================================================
def run_demo():
    """运行演示"""
    print("=" * 70)
    print("📊 量化因子计算器 Demo")
    print("=" * 70)
    
    # 创建模拟数据
    np.random.seed(42)
    n_days = 60
    dates = pd.date_range('2025-01-01', periods=n_days, freq='B')
    
    # 模拟正常股票价格 (带有随机波动)
    base_price = 100
    returns = np.random.normal(0.001, 0.02, n_days)  # 日收益率
    close = base_price * np.exp(np.cumsum(returns))
    
    # 模拟开盘价 (收盘价 + 随机隔夜波动)
    overnight_gap = np.random.normal(0, 0.005, n_days)
    open_price = np.roll(close, 1) * np.exp(overnight_gap)
    open_price[0] = close[0] * 0.99
    
    # 模拟最高/最低价
    high = np.maximum(open_price, close) * (1 + np.abs(np.random.normal(0, 0.01, n_days)))
    low = np.minimum(open_price, close) * (1 - np.abs(np.random.normal(0, 0.01, n_days)))
    
    # 模拟成交量 (百万股)
    volume = np.random.lognormal(10, 0.5, n_days)
    
    # 模拟大盘指数
    benchmark_returns = np.random.normal(0.0005, 0.01, n_days)
    benchmark_close = 3000 * np.exp(np.cumsum(benchmark_returns))
    
    # 构建 DataFrame
    df = pd.DataFrame({
        'Open': open_price,
        'High': high,
        'Low': low,
        'Close': close,
        'Volume': volume,
        'Benchmark_Close': benchmark_close
    }, index=dates)
    
    print("\n📋 模拟数据 (前5行):")
    print(df.head())
    
    # 计算因子
    print("\n🔬 计算量化因子 (窗口=20天)...")
    qf = QuantFactors(df, window=20)
    factors = qf.calc_all_factors()
    
    print("\n📈 因子结果 (最后10行):")
    print(factors.tail(10).to_string())
    
    # 统计信息
    print("\n📊 因子统计:")
    print(factors.describe().round(4))
    
    # 计算庄股评分
    print("\n🎯 庄股识别评分 (最后5天):")
    score = qf.calc_manipulation_score()
    print(score.tail(5))
    
    # 风险提示
    latest_score = score.iloc[-1]
    if latest_score >= 75:
        print(f"\n⚠️ 警告: 庄股风险评分 {latest_score:.0f}分 (高风险)")
    elif latest_score >= 50:
        print(f"\n⚡ 注意: 庄股风险评分 {latest_score:.0f}分 (中等风险)")
    else:
        print(f"\n✅ 正常: 庄股风险评分 {latest_score:.0f}分 (低风险)")
    
    print("\n" + "=" * 70)
    print("Demo 完成!")
    print("=" * 70)
    
    return df, factors


if __name__ == '__main__':
    run_demo()
