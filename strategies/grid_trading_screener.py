# -*- coding: utf-8 -*-
"""
网格交易标的筛选器
- 筛选适合网格化交易的股票和ETF
- 分析历年3月份行情特征
- 分析两会前后交易特征

网格交易适合的标的特征：
1. 价格在一定区间内波动（震荡市）
2. 波动率适中（年化15%-40%）
3. 流动性好（日均成交额 > 5000万）
4. 无明显单边趋势（横盘或宽幅震荡）
5. 有一定的波动规律性

两会时间：通常在3月初（3月3日-3月15日左右）
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import logging
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger('momentum')


class GridTradingScreener:
    """网格交易标的筛选器"""
    
    def __init__(self, lookback_years: int = 3):
        """
        初始化筛选器
        
        Args:
            lookback_years: 回看年数，用于分析历史数据
        """
        self.lookback_years = lookback_years
        
    def calculate_volatility(self, prices: pd.Series) -> float:
        """
        计算年化波动率
        
        Args:
            prices: 价格序列
            
        Returns:
            年化波动率（%）
        """
        if len(prices) < 2:
            return 0.0
            
        returns = prices.pct_change().dropna()
        if len(returns) == 0:
            return 0.0
            
        # 年化波动率 = 日波动率 * sqrt(252)
        daily_vol = returns.std()
        annual_vol = daily_vol * np.sqrt(252) * 100
        
        return annual_vol
    
    def calculate_price_range_stability(self, df: pd.DataFrame) -> Dict:
        """
        计算价格区间稳定性指标
        
        Args:
            df: 包含 high, low, close 的DataFrame
            
        Returns:
            区间指标字典
        """
        if df.empty or len(df) < 20:
            return {
                'range_ratio': 0,
                'is_ranging': False,
                'support': 0,
                'resistance': 0
            }
        
        # 复制DataFrame以避免SettingWithCopyWarning
        df = df.copy()
        
        high = df['high'].max()
        low = df['low'].min()
        current = df['close'].iloc[-1]
        
        # 价格区间比率（从低点到高点的涨幅）
        range_ratio = (high - low) / low * 100 if low > 0 else 0
        
        # 计算支撑位和阻力位（使用20日滚动窗口的最低/最高价）
        df['support_20'] = df['low'].rolling(window=20).min()
        df['resistance_20'] = df['high'].rolling(window=20).max()
        
        support = df['support_20'].iloc[-1]
        resistance = df['resistance_20'].iloc[-1]
        
        # 判断是否在区间内震荡（当前价格在区间中部±30%以内）
        mid_price = (high + low) / 2
        price_position = abs(current - mid_price) / (high - low) if (high - low) > 0 else 1
        is_ranging = price_position < 0.3  # 价格在区间中部附近
        
        # 计算价格在支撑和阻力之间的位置（0-1）
        if resistance > support:
            position_in_range = (current - support) / (resistance - support)
        else:
            position_in_range = 0.5
        
        return {
            'range_ratio': range_ratio,
            'is_ranging': is_ranging,
            'support': support,
            'resistance': resistance,
            'position_in_range': position_in_range,
            'price_position': price_position
        }
    
    def calculate_trend_strength(self, prices: pd.Series) -> float:
        """
        计算趋势强度（值越小越适合网格交易）
        使用线性回归的R²值
        
        Args:
            prices: 价格序列
            
        Returns:
            R²值（0-1），越接近0表示越横盘
        """
        if len(prices) < 10:
            return 1.0
        
        prices_array = prices.values
        x = np.arange(len(prices_array))
        
        # 线性回归
        coeffs = np.polyfit(x, prices_array, 1)
        predicted = np.polyval(coeffs, x)
        
        # 计算R²
        ss_res = np.sum((prices_array - predicted) ** 2)
        ss_tot = np.sum((prices_array - np.mean(prices_array)) ** 2)
        
        if ss_tot == 0:
            return 1.0
        
        r_squared = 1 - (ss_res / ss_tot)
        
        return abs(r_squared)  # 返回绝对值
    
    def calculate_liquidity(self, df: pd.DataFrame) -> Dict:
        """
        计算流动性指标
        
        Args:
            df: 包含 volume, close 的DataFrame
            
        Returns:
            流动性指标字典
        """
        if df.empty:
            return {
                'avg_turnover': 0,
                'avg_volume': 0,
                'is_liquid': False
            }
        
        # 复制DataFrame以避免警告
        df = df.copy()
        
        # 日均成交额（万元）
        df['turnover'] = df['volume'] * df['close'] / 10000
        avg_turnover = df['turnover'].mean()
        avg_volume = df['volume'].mean()
        
        # 流动性标准：日均成交额 > 5000万
        is_liquid = avg_turnover > 5000
        
        return {
            'avg_turnover': avg_turnover,
            'avg_volume': avg_volume,
            'is_liquid': is_liquid
        }
    
    def analyze_march_performance(self, df: pd.DataFrame, years: int = 3) -> Dict:
        """
        分析历年3月份表现
        
        Args:
            df: 历史行情数据，需要包含date或trade_date列
            years: 分析年数
            
        Returns:
            3月份表现指标
        """
        if df.empty:
            return {
                'march_avg_return': 0,
                'march_volatility': 0,
                'march_win_rate': 0,
                'march_data_points': 0
            }
        
        # 兼容date和trade_date两种列名
        date_col = 'date' if 'date' in df.columns else 'trade_date'
        if date_col not in df.columns:
            return {
                'march_avg_return': 0,
                'march_volatility': 0,
                'march_win_rate': 0,
                'march_data_points': 0
            }
        
        # 复制DataFrame避免修改原数据
        df = df.copy()
        
        # 确保date列是datetime类型
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col).sort_index()
        
        # 筛选3月份数据
        march_data = df[df.index.month == 3]
        
        if len(march_data) < 5:
            return {
                'march_avg_return': 0,
                'march_volatility': 0,
                'march_win_rate': 0,
                'march_data_points': len(march_data)
            }
        
        # 计算每年3月份的收益
        march_returns = []
        for year in range(datetime.now().year - years, datetime.now().year):
            year_march = march_data[march_data.index.year == year]
            if len(year_march) >= 2:
                start_price = year_march['close'].iloc[0]
                end_price = year_march['close'].iloc[-1]
                ret = (end_price - start_price) / start_price * 100
                march_returns.append(ret)
        
        if not march_returns:
            return {
                'march_avg_return': 0,
                'march_volatility': 0,
                'march_win_rate': 0,
                'march_data_points': len(march_data)
            }
        
        # 计算3月份平均收益率和波动率
        march_avg_return = np.mean(march_returns)
        march_volatility = np.std(march_returns)
        
        # 3月份胜率（正收益的年份占比）
        march_win_rate = sum(1 for r in march_returns if r > 0) / len(march_returns) * 100
        
        return {
            'march_avg_return': march_avg_return,
            'march_volatility': march_volatility,
            'march_win_rate': march_win_rate,
            'march_data_points': len(march_data),
            'march_yearly_returns': march_returns
        }
    
    def analyze_two_sessions_period(self, df: pd.DataFrame, years: int = 3) -> Dict:
        """
        分析两会期间（3月1日-3月20日）的行情特征
        
        Args:
            df: 历史行情数据
            years: 分析年数
            
        Returns:
            两会期间表现指标
        """
        if df.empty:
            return {
                'sessions_avg_return': 0,
                'sessions_volatility': 0,
                'sessions_max_drawdown': 0,
                'sessions_suitable_for_grid': False
            }
        
        # 兼容date和trade_date两种列名
        date_col = 'date' if 'date' in df.columns else 'trade_date'
        if date_col not in df.columns:
            return {
                'sessions_avg_return': 0,
                'sessions_volatility': 0,
                'sessions_max_drawdown': 0,
                'sessions_suitable_for_grid': False
            }
        
        # 复制DataFrame避免修改原数据
        df = df.copy()
        
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col).sort_index()
        
        # 筛选两会期间数据（3月1日-3月20日）
        sessions_data = df[(df.index.month == 3) & (df.index.day <= 20)]
        
        if len(sessions_data) < 5:
            return {
                'sessions_avg_return': 0,
                'sessions_volatility': 0,
                'sessions_max_drawdown': 0,
                'sessions_suitable_for_grid': False
            }
        
        # 计算每年两会期间的表现
        sessions_returns = []
        sessions_vols = []
        sessions_drawdowns = []
        
        for year in range(datetime.now().year - years, datetime.now().year):
            year_sessions = sessions_data[sessions_data.index.year == year]
            
            if len(year_sessions) >= 3:
                # 收益率
                start_price = year_sessions['close'].iloc[0]
                end_price = year_sessions['close'].iloc[-1]
                ret = (end_price - start_price) / start_price * 100
                sessions_returns.append(ret)
                
                # 期间波动率
                daily_returns = year_sessions['close'].pct_change().dropna()
                vol = daily_returns.std() * 100
                sessions_vols.append(vol)
                
                # 最大回撤
                cummax = year_sessions['close'].cummax()
                drawdown = ((year_sessions['close'] - cummax) / cummax * 100).min()
                sessions_drawdowns.append(abs(drawdown))
        
        if not sessions_returns:
            return {
                'sessions_avg_return': 0,
                'sessions_volatility': 0,
                'sessions_max_drawdown': 0,
                'sessions_suitable_for_grid': False
            }
        
        sessions_avg_return = np.mean(sessions_returns)
        sessions_volatility = np.mean(sessions_vols)
        sessions_max_drawdown = np.mean(sessions_drawdowns)
        
        # 判断是否适合网格：波动适中、回撤可控、收益稳定
        suitable_for_grid = (
            1.5 <= sessions_volatility <= 5.0 and  # 日波动1.5%-5%
            sessions_max_drawdown < 15 and  # 回撤小于15%
            abs(sessions_avg_return) < 20  # 不是单边大行情
        )
        
        return {
            'sessions_avg_return': sessions_avg_return,
            'sessions_volatility': sessions_volatility,
            'sessions_max_drawdown': sessions_max_drawdown,
            'sessions_suitable_for_grid': suitable_for_grid,
            'sessions_yearly_returns': sessions_returns
        }
    
    def calculate_grid_score(self, metrics: Dict) -> float:
        """
        计算网格交易综合评分（0-100分）
        
        Args:
            metrics: 各项指标字典
            
        Returns:
            综合评分
        """
        score = 0
        
        # 1. 波动率得分（15%-40%最佳）- 30分
        vol = metrics.get('volatility', 0)
        if 15 <= vol <= 40:
            vol_score = 30
        elif 10 <= vol < 15 or 40 < vol <= 50:
            vol_score = 20
        elif 5 <= vol < 10 or 50 < vol <= 60:
            vol_score = 10
        else:
            vol_score = 0
        score += vol_score
        
        # 2. 趋势强度得分（R²越小越好）- 25分
        trend_strength = metrics.get('trend_strength', 1)
        if trend_strength < 0.3:
            trend_score = 25
        elif trend_strength < 0.5:
            trend_score = 20
        elif trend_strength < 0.7:
            trend_score = 10
        else:
            trend_score = 0
        score += trend_score
        
        # 3. 流动性得分 - 20分
        if metrics.get('is_liquid', False):
            score += 20
        
        # 4. 区间震荡得分 - 15分
        if metrics.get('is_ranging', False):
            score += 15
        
        # 5. 两会期间适合度 - 10分
        if metrics.get('sessions_suitable_for_grid', False):
            score += 10
        
        return score
    
    def screen_single_stock(self, code: str, name: str, df: pd.DataFrame) -> Optional[Dict]:
        """
        筛选单个股票/ETF
        
        Args:
            code: 股票代码
            name: 股票名称
            df: 历史行情数据
            
        Returns:
            评估结果字典，不符合条件返回None
        """
        if df.empty or len(df) < 120:  # 至少需要半年数据
            return None
        
        try:
            # 1. 计算波动率
            volatility = self.calculate_volatility(df['close'])
            
            # 2. 计算流动性
            liquidity = self.calculate_liquidity(df)
            if not liquidity['is_liquid']:
                return None  # 流动性不足，直接过滤
            
            # 3. 计算趋势强度
            trend_strength = self.calculate_trend_strength(df['close'].tail(120))
            
            # 4. 计算价格区间指标
            range_metrics = self.calculate_price_range_stability(df.tail(120))
            
            # 5. 分析3月份表现
            march_metrics = self.analyze_march_performance(df, self.lookback_years)
            
            # 6. 分析两会期间表现
            sessions_metrics = self.analyze_two_sessions_period(df, self.lookback_years)
            
            # 整合所有指标
            metrics = {
                'code': code,
                'name': name,
                'volatility': volatility,
                'trend_strength': trend_strength,
                **range_metrics,
                **liquidity,
                **march_metrics,
                **sessions_metrics
            }
            
            # 计算综合评分
            metrics['grid_score'] = self.calculate_grid_score(metrics)
            
            # 只返回评分>=60的标的
            if metrics['grid_score'] >= 60:
                return metrics
            
        except Exception as e:
            logger.error(f"筛选 {code} 出错: {e}")
            return None
        
        return None
    
    def format_screening_result(self, results: List[Dict]) -> pd.DataFrame:
        """
        格式化筛选结果
        
        Args:
            results: 筛选结果列表
            
        Returns:
            格式化的DataFrame
        """
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        
        # 选择关键列
        columns = [
            'code', 'name', 'grid_score', 'volatility', 'trend_strength',
            'avg_turnover', 'range_ratio', 'position_in_range',
            'march_avg_return', 'march_win_rate',
            'sessions_avg_return', 'sessions_volatility', 'sessions_max_drawdown',
            'sessions_suitable_for_grid'
        ]
        
        # 只保留存在的列
        columns = [col for col in columns if col in df.columns]
        df = df[columns]
        
        # 按评分降序排序
        df = df.sort_values('grid_score', ascending=False)
        
        # 重命名列（中文）
        rename_map = {
            'code': '代码',
            'name': '名称',
            'grid_score': '网格评分',
            'volatility': '年化波动率%',
            'trend_strength': '趋势强度',
            'avg_turnover': '日均成交额(万)',
            'range_ratio': '价格区间幅度%',
            'position_in_range': '当前位置',
            'march_avg_return': '3月平均收益%',
            'march_win_rate': '3月胜率%',
            'sessions_avg_return': '两会收益%',
            'sessions_volatility': '两会波动率%',
            'sessions_max_drawdown': '两会最大回撤%',
            'sessions_suitable_for_grid': '两会期间适合'
        }
        
        df = df.rename(columns=rename_map)
        
        # 格式化数值
        float_cols = [col for col in df.columns if df[col].dtype == 'float64']
        for col in float_cols:
            df[col] = df[col].round(2)
        
        return df


def main():
    """主程序 - 示例用法"""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    try:
        import adata
    except ImportError:
        print("错误: 无法导入adata模块，请确保已安装")
        return
    
    print("=" * 80)
    print("网格交易标的筛选器 v1.0")
    print("=" * 80)
    print("\n筛选标准:")
    print("1. 年化波动率: 15%-40% (适中波动)")
    print("2. 趋势强度: R² < 0.5 (横盘震荡)")
    print("3. 流动性: 日均成交额 > 5000万")
    print("4. 历年3月份和两会期间表现分析")
    print("-" * 80)
    
    # 初始化筛选器
    screener = GridTradingScreener(lookback_years=3)
    
    # 示例：筛选ETF（ETF通常更适合网格交易）
    print("\n正在获取ETF列表...")
    
    try:
        # 获取股票列表（这里可以替换为ETF列表）
        # stock_list = adata.stock.info.all_code()
        
        # 示例：手动指定一些常见ETF代码进行测试
        test_etfs = [
            ('510300', '300ETF'),
            ('510500', '500ETF'),
            ('159915', '创业板ETF'),
            ('512000', '券商ETF'),
            ('159992', '创成长'),
            ('159949', '创业板50'),
        ]
        
        print(f"开始筛选 {len(test_etfs)} 个标的...\n")
        
        results = []
        
        from momentum.data import load_or_fetch_kline
        from momentum.data.fetcher import fetch_kline_from_api
        
        for code, name in test_etfs:
            print(f"分析 {code} {name}...", end=' ')
            
            # 获取历史数据（近3年）
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365 * 3)
            
            try:
                df = load_or_fetch_kline(
                    code=code,
                    fetch_func=fetch_kline_from_api,
                    start_date=start_date.strftime('%Y-%m-%d')
                )
                
                if df is not None and not df.empty:
                    result = screener.screen_single_stock(code, name, df)
                    if result:
                        results.append(result)
                        print(f"✓ 评分: {result['grid_score']:.0f}")
                    else:
                        print("× 不符合条件")
                else:
                    print("× 数据获取失败")
                    
            except Exception as e:
                print(f"× 错误: {e}")
                continue
        
        # 输出结果
        print("\n" + "=" * 80)
        print("筛选结果")
        print("=" * 80)
        
        if results:
            df_result = screener.format_screening_result(results)
            print(f"\n共筛选出 {len(df_result)} 个符合条件的标的:")
            print(df_result.to_string(index=False))
            
            # 保存结果
            output_file = f'grid_trading_candidates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            df_result.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n结果已保存至: {output_file}")
        else:
            print("\n未找到符合条件的标的")
            
    except Exception as e:
        logger.error(f"筛选过程出错: {e}", exc_info=True)
        print(f"\n错误: {e}")


if __name__ == '__main__':
    main()
