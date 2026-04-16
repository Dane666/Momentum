# -*- coding: utf-8 -*-
"""
持仓周期参数优化工具

提供：
1. 多周期对比测试
2. 最优周期推荐
3. 周期敏感性分析
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger('momentum.backtest')


class HoldPeriodOptimizer:
    """
    持仓周期优化器
    
    使用示例:
    ```python
    optimizer = HoldPeriodOptimizer(backtest_days=120)
    
    # 运行多周期测试
    results = optimizer.run_multi_period_test([2, 3, 4, 5])
    
    # 获取推荐
    best = optimizer.recommend_period(results)
    print(f"推荐周期: {best['period']} 天")
    ```
    """
    
    def __init__(self, backtest_days: int = 120):
        """
        初始化优化器
        
        Args:
            backtest_days: 回测天数
        """
        self.backtest_days = backtest_days
        self.results_cache: Dict[int, Dict] = {}
    
    def run_multi_period_test(
        self,
        periods: List[int] = None,
        window_shift: int = 0,
        verbose: bool = True
    ) -> pd.DataFrame:
        """
        运行多周期对比测试
        
        Args:
            periods: 持仓周期列表
            window_shift: 窗口偏移
            verbose: 是否打印进度
            
        Returns:
            结果DataFrame
        """
        from ..backtest import MomentumBacktester
        
        if periods is None:
            periods = [2, 3, 4, 5, 7]
        
        results = []
        
        for period in periods:
            if verbose:
                logger.info(f"测试持仓周期: {period} 天")
            
            try:
                tester = MomentumBacktester(
                    backtest_days=self.backtest_days,
                    hold_period=period,
                    window_shift=window_shift,
                    record_trades=False  # 不记录到DB加速测试
                )
                metrics = tester.run_backtest()
                
                if metrics:
                    result = {
                        'hold_period': period,
                        'total_return': metrics.get('profit_pct', 0),
                        'annual_return': metrics.get('annual_ret', 0),
                        'sharpe': metrics.get('sharpe', 0),
                        'max_drawdown': metrics.get('max_dd', 0),
                        'win_rate': metrics.get('win_rate', 0),
                        'trade_count': metrics.get('trade_count', 0),
                        'final_nav': metrics.get('final_nav', 1.0),
                    }
                    results.append(result)
                    self.results_cache[period] = result
                    
            except Exception as e:
                logger.error(f"周期 {period} 测试失败: {e}")
        
        df = pd.DataFrame(results)
        
        if verbose and not df.empty:
            self._print_results(df)
        
        return df
    
    def _print_results(self, df: pd.DataFrame):
        """打印结果表格"""
        print("\n" + "=" * 90)
        print(f"📊 持仓周期对比分析 (回测 {self.backtest_days} 天)")
        print("-" * 90)
        print(f"{'周期(天)':<10} {'总收益%':<12} {'年化%':<12} {'夏普':<10} "
              f"{'回撤%':<10} {'胜率%':<10} {'交易数':<8}")
        print("-" * 90)
        
        for _, row in df.iterrows():
            print(f"{row['hold_period']:<10} {row['total_return']:<12.2f} "
                  f"{row['annual_return']:<12.2f} {row['sharpe']:<10.2f} "
                  f"{row['max_drawdown']:<10.2f} {row['win_rate']:<10.2f} "
                  f"{row['trade_count']:<8}")
        
        print("=" * 90)
    
    def recommend_period(
        self,
        results: pd.DataFrame = None,
        optimize_for: str = 'sharpe'
    ) -> Dict:
        """
        推荐最优持仓周期
        
        Args:
            results: 测试结果DataFrame
            optimize_for: 优化目标 (sharpe/profit/calmar)
            
        Returns:
            推荐结果字典
        """
        if results is None:
            results = pd.DataFrame(list(self.results_cache.values()))
        
        if results.empty:
            return {'period': 3, 'reason': '无测试数据，使用默认值'}
        
        # 根据不同目标选择最优
        if optimize_for == 'sharpe':
            best_idx = results['sharpe'].idxmax()
            metric_value = results.loc[best_idx, 'sharpe']
            reason = f"夏普比率最高 ({metric_value:.2f})"
            
        elif optimize_for == 'profit':
            best_idx = results['total_return'].idxmax()
            metric_value = results.loc[best_idx, 'total_return']
            reason = f"总收益最高 ({metric_value:.2f}%)"
            
        elif optimize_for == 'calmar':
            results['calmar'] = results['annual_return'] / (results['max_drawdown'] + 0.01)
            best_idx = results['calmar'].idxmax()
            metric_value = results.loc[best_idx, 'calmar']
            reason = f"卡玛比率最高 ({metric_value:.2f})"
            
        else:
            # 综合评分
            results['score'] = (
                results['sharpe'] / results['sharpe'].max() * 0.4 +
                results['total_return'] / results['total_return'].max() * 0.3 +
                (1 - results['max_drawdown'] / results['max_drawdown'].max()) * 0.3
            )
            best_idx = results['score'].idxmax()
            reason = "综合评分最高"
        
        best_period = int(results.loc[best_idx, 'hold_period'])
        
        return {
            'period': best_period,
            'reason': reason,
            'sharpe': results.loc[best_idx, 'sharpe'],
            'total_return': results.loc[best_idx, 'total_return'],
            'max_drawdown': results.loc[best_idx, 'max_drawdown'],
            'win_rate': results.loc[best_idx, 'win_rate'],
        }
    
    def analyze_stability(
        self,
        period: int,
        window_shifts: List[int] = None
    ) -> Dict:
        """
        分析特定周期的稳定性
        
        通过滑动窗口测试检验结果是否稳定
        
        Args:
            period: 持仓周期
            window_shifts: 窗口偏移列表
            
        Returns:
            稳定性分析结果
        """
        from ..backtest import MomentumBacktester
        
        if window_shifts is None:
            window_shifts = [0, 1, 2, 3, 5]
        
        results = []
        
        for shift in window_shifts:
            try:
                tester = MomentumBacktester(
                    backtest_days=self.backtest_days,
                    hold_period=period,
                    window_shift=shift,
                    record_trades=False
                )
                metrics = tester.run_backtest()
                
                if metrics:
                    results.append({
                        'shift': shift,
                        'sharpe': metrics.get('sharpe', 0),
                        'profit': metrics.get('profit_pct', 0),
                    })
            except Exception as e:
                logger.warning(f"窗口偏移 {shift} 测试失败: {e}")
        
        if not results:
            return {'is_stable': False, 'reason': '无有效测试结果'}
        
        df = pd.DataFrame(results)
        
        # 计算稳定性指标
        sharpe_std = df['sharpe'].std()
        sharpe_mean = df['sharpe'].mean()
        profit_std = df['profit'].std()
        profit_mean = df['profit'].mean()
        
        # 变异系数 (CV) < 0.3 认为稳定
        sharpe_cv = sharpe_std / (sharpe_mean + 0.01)
        profit_cv = profit_std / (abs(profit_mean) + 0.01)
        
        is_stable = sharpe_cv < 0.3 and profit_cv < 0.5
        
        return {
            'is_stable': is_stable,
            'sharpe_mean': sharpe_mean,
            'sharpe_std': sharpe_std,
            'sharpe_cv': sharpe_cv,
            'profit_mean': profit_mean,
            'profit_std': profit_std,
            'profit_cv': profit_cv,
            'test_count': len(results),
            'reason': '稳定' if is_stable else f'波动较大(夏普CV={sharpe_cv:.2f})',
        }


def run_period_optimization(
    backtest_days: int = 120,
    periods: List[int] = None,
    optimize_for: str = 'sharpe'
) -> Tuple[int, Dict]:
    """
    便捷函数：运行持仓周期优化
    
    Args:
        backtest_days: 回测天数
        periods: 测试周期列表
        optimize_for: 优化目标
        
    Returns:
        (最优周期, 详细结果)
    """
    optimizer = HoldPeriodOptimizer(backtest_days)
    results = optimizer.run_multi_period_test(periods)
    recommendation = optimizer.recommend_period(results, optimize_for)
    
    return recommendation['period'], recommendation


def quick_period_test() -> Dict:
    """
    快速周期测试 (使用缩短的回测天数)
    
    Returns:
        测试结果摘要
    """
    optimizer = HoldPeriodOptimizer(backtest_days=60)  # 缩短回测加速
    results = optimizer.run_multi_period_test([2, 3, 5])
    return optimizer.recommend_period(results)
