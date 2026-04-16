# -*- coding: utf-8 -*-
"""
回测稳定性验证模块

解决回测窗口敏感性问题:
1. 滑动窗口回测 - 验证策略在不同起始日期的表现一致性
2. 未来函数检测 - 自动检查是否使用了未来数据
3. 股票集中度分析 - 检测是否依赖个别妖股
4. 参数稳定性测试 - 检验参数对小扰动的敏感度
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger('momentum')


class StabilityAnalyzer:
    """
    回测稳定性分析器
    
    核心功能:
    1. 滑动窗口回测 (Rolling Window Backtest)
    2. 收益集中度分析 (Concentration Analysis)
    3. 未来函数检测 (Look-Ahead Bias Detection)
    """
    
    def __init__(self, backtest_days: int = 120, hold_period: int = 3):
        """
        初始化稳定性分析器
        
        Args:
            backtest_days: 每个窗口的回测天数
            hold_period: 持仓周期
        """
        self.backtest_days = backtest_days
        self.hold_period = hold_period
    
    def run_rolling_window_backtest(
        self,
        window_shifts: Optional[List[int]] = None,
        parallel: bool = True
    ) -> pd.DataFrame:
        """
        滑动窗口回测 - 检验策略的时间稳定性
        
        原理: 将回测窗口向前滑动不同的天数，观察收益率的波动。
        如果策略稳健，不同起始日的收益率应该相近。
        
        Args:
            window_shifts: 窗口偏移列表 (相对于今天向前偏移的天数)
                          例如 [0, 1, 2, 3, 4] 表示今天、昨天、前天...开始
            parallel: 是否并行执行
            
        Returns:
            包含各窗口回测结果的 DataFrame
        """
        from .simulator import MomentumBacktester
        
        if window_shifts is None:
            # 默认检查近5天的滑动窗口
            window_shifts = [0, 1, 2, 3, 4, 5, 10, 20]
        
        logger.info("=" * 60)
        logger.info(f"🔄 滑动窗口稳定性分析")
        logger.info(f"   回测天数: {self.backtest_days}, 持仓周期: {self.hold_period}")
        logger.info(f"   窗口偏移: {window_shifts}")
        logger.info("=" * 60)
        
        results = []
        
        def run_single_window(shift: int) -> Optional[Dict]:
            """运行单个窗口的回测"""
            try:
                # 创建一个带偏移的回测器
                bt = MomentumBacktester(
                    backtest_days=self.backtest_days,
                    hold_period=self.hold_period,
                    record_trades=False
                )
                # 设置偏移 (需要在 simulator 中支持)
                bt._window_shift = shift
                
                result = bt.run_backtest()
                if result:
                    result['window_shift'] = shift
                    result['start_date'] = (datetime.now() - timedelta(days=shift + self.backtest_days)).strftime('%Y-%m-%d')
                    return result
            except Exception as e:
                logger.warning(f"窗口偏移 {shift} 回测失败: {e}")
            return None
        
        if parallel:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(run_single_window, s) for s in window_shifts]
                for f in futures:
                    r = f.result()
                    if r:
                        results.append(r)
        else:
            for shift in window_shifts:
                r = run_single_window(shift)
                if r:
                    results.append(r)
        
        if not results:
            logger.error("滑动窗口回测无有效结果")
            return pd.DataFrame()
        
        df = pd.DataFrame(results).sort_values('window_shift')
        
        # 计算稳定性指标
        ret_mean = df['profit_pct'].mean()
        ret_std = df['profit_pct'].std()
        ret_cv = ret_std / (abs(ret_mean) + 1e-9)  # 变异系数
        ret_range = df['profit_pct'].max() - df['profit_pct'].min()
        
        logger.info("\n" + "=" * 60)
        logger.info("📊 滑动窗口稳定性报告")
        logger.info("-" * 60)
        logger.info(f"{'偏移(天)':<10} {'起始日':<12} {'收益%':<10} {'夏普':<8} {'胜率%':<8}")
        logger.info("-" * 60)
        
        for _, row in df.iterrows():
            logger.info(f"{row['window_shift']:<10} {row['start_date']:<12} "
                       f"{row['profit_pct']:<10.2f} {row['sharpe']:<8.2f} {row['win_rate']:<8.1f}")
        
        logger.info("-" * 60)
        logger.info(f"📈 收益率统计:")
        logger.info(f"   均值: {ret_mean:.2f}%  标准差: {ret_std:.2f}%")
        logger.info(f"   极差: {ret_range:.2f}%  变异系数: {ret_cv:.2f}")
        
        # 稳定性评级
        if ret_cv < 0.3 and ret_range < 30:
            stability_grade = "🟢 优秀 (策略稳定)"
        elif ret_cv < 0.5 and ret_range < 50:
            stability_grade = "🟡 一般 (存在波动)"
        else:
            stability_grade = "🔴 不稳定 (需要检查)"
        
        logger.info(f"   稳定性评级: {stability_grade}")
        logger.info("=" * 60)
        
        return df
    
    def analyze_concentration(self, session_id: Optional[str] = None) -> Dict:
        """
        分析收益集中度 - 检测是否过度依赖个别妖股
        
        Args:
            session_id: 回测会话ID，用于从数据库读取交易记录
            
        Returns:
            集中度分析结果
        """
        from ..data.db import get_db_connection
        
        logger.info("=" * 60)
        logger.info("🎯 收益集中度分析")
        logger.info("=" * 60)
        
        try:
            with get_db_connection() as conn:
                if session_id:
                    query = """
                        SELECT code, name, 
                               COUNT(*) as trade_count,
                               SUM((sell_price - buy_price) / buy_price * 100) as total_return_pct,
                               AVG((sell_price - buy_price) / buy_price * 100) as avg_return_pct
                        FROM backtest_trades
                        WHERE session_id = ?
                        GROUP BY code
                        ORDER BY total_return_pct DESC
                    """
                    df = pd.read_sql_query(query, conn, params=(session_id,))
                else:
                    # 获取最新会话
                    query = """
                        SELECT code, name, 
                               COUNT(*) as trade_count,
                               SUM((sell_price - buy_price) / buy_price * 100) as total_return_pct,
                               AVG((sell_price - buy_price) / buy_price * 100) as avg_return_pct
                        FROM backtest_trades
                        WHERE session_id = (SELECT session_id FROM backtest_sessions ORDER BY created_at DESC LIMIT 1)
                        GROUP BY code
                        ORDER BY total_return_pct DESC
                    """
                    df = pd.read_sql_query(query, conn)
            
            if df.empty:
                logger.warning("无交易记录可分析")
                return {}
            
            # 计算集中度指标
            total_return = df['total_return_pct'].sum()
            top3_return = df.head(3)['total_return_pct'].sum()
            top5_return = df.head(5)['total_return_pct'].sum()
            top10_return = df.head(10)['total_return_pct'].sum()
            
            concentration_top3 = top3_return / total_return * 100 if total_return > 0 else 0
            concentration_top5 = top5_return / total_return * 100 if total_return > 0 else 0
            concentration_top10 = top10_return / total_return * 100 if total_return > 0 else 0
            
            logger.info("\n收益贡献Top10:")
            logger.info(f"{'代码':<10} {'名称':<12} {'交易次数':<8} {'总收益%':<10} {'均收益%':<10}")
            logger.info("-" * 60)
            
            for _, row in df.head(10).iterrows():
                logger.info(f"{row['code']:<10} {row['name']:<12} {row['trade_count']:<8} "
                           f"{row['total_return_pct']:<10.2f} {row['avg_return_pct']:<10.2f}")
            
            logger.info("-" * 60)
            logger.info(f"Top3 集中度: {concentration_top3:.1f}%")
            logger.info(f"Top5 集中度: {concentration_top5:.1f}%")
            logger.info(f"Top10 集中度: {concentration_top10:.1f}%")
            
            # 集中度评级
            if concentration_top3 > 50:
                grade = "🔴 严重依赖 (策略不可靠)"
            elif concentration_top5 > 60:
                grade = "🟡 轻度依赖 (需关注)"
            else:
                grade = "🟢 分散良好 (策略健康)"
            
            logger.info(f"集中度评级: {grade}")
            logger.info("=" * 60)
            
            return {
                'total_stocks': len(df),
                'total_return': total_return,
                'concentration_top3': concentration_top3,
                'concentration_top5': concentration_top5,
                'concentration_top10': concentration_top10,
                'top_contributors': df.head(5).to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"集中度分析失败: {e}")
            return {}


def detect_lookahead_bias(df: pd.DataFrame, signal_col: str = 'signal') -> List[str]:
    """
    检测未来函数 (Look-Ahead Bias)
    
    检查信号生成是否使用了未来数据
    
    Args:
        df: 包含信号和价格的 DataFrame
        signal_col: 信号列名称
        
    Returns:
        检测到的问题列表
    """
    issues = []
    
    # 检查1: 信号是否与未来收益高度相关
    if signal_col in df.columns and 'close' in df.columns:
        df = df.copy()
        df['future_ret'] = df['close'].shift(-1) / df['close'] - 1
        
        # 计算信号与未来收益的相关性
        corr = df[signal_col].corr(df['future_ret'])
        
        if abs(corr) > 0.5:
            issues.append(f"⚠️ 信号与未来1日收益相关性过高 (corr={corr:.3f})，可能存在未来函数")
    
    # 检查2: 信号是否使用了当日收盘价
    # 这需要代码审计，这里提供建议
    issues.append("💡 建议: 检查信号计算是否用了 df['close'].iloc[-1] 而非 df['close'].shift(1).iloc[-1]")
    
    return issues


def run_stability_check(
    backtest_days: int = 120,
    hold_period: int = 3,
    run_concentration: bool = True
) -> Dict:
    """
    运行完整的稳定性检查
    
    Args:
        backtest_days: 回测天数
        hold_period: 持仓周期
        run_concentration: 是否分析集中度
        
    Returns:
        完整的稳定性分析结果
    """
    analyzer = StabilityAnalyzer(backtest_days, hold_period)
    
    results = {}
    
    # 1. 滑动窗口分析
    results['rolling_window'] = analyzer.run_rolling_window_backtest()
    
    # 2. 集中度分析
    if run_concentration:
        results['concentration'] = analyzer.analyze_concentration()
    
    return results
