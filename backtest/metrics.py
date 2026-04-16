# -*- coding: utf-8 -*-
"""
回测指标计算模块

从 simulator.py 提取的性能指标计算逻辑，
遵循单一职责原则，便于测试和复用。
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger('momentum.backtest')


@dataclass
class BacktestMetrics:
    """回测性能指标数据类"""
    total_return: float      # 总收益率 (%)
    annual_return: float     # 年化收益率 (%)
    sharpe_ratio: float      # 夏普比率
    max_drawdown: float      # 最大回撤 (%)
    win_rate: float          # 胜率 (%)
    trade_count: int         # 交易总数
    avg_profit: float        # 平均单笔收益 (%)
    profit_factor: float     # 盈亏比
    final_nav: float         # 最终净值
    calmar_ratio: float      # 卡玛比率 (年化收益/最大回撤)
    sortino_ratio: float     # 索提诺比率
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_return': self.total_return,
            'annual_return': self.annual_return,
            'sharpe_ratio': self.sharpe_ratio,
            'max_drawdown': self.max_drawdown,
            'win_rate': self.win_rate,
            'trade_count': self.trade_count,
            'avg_profit': self.avg_profit,
            'profit_factor': self.profit_factor,
            'final_nav': self.final_nav,
            'calmar_ratio': self.calmar_ratio,
            'sortino_ratio': self.sortino_ratio,
        }
    
    def is_valid(self) -> bool:
        """验证指标是否在合理范围内"""
        return (
            -100 <= self.total_return <= 1000 and
            -100 <= self.annual_return <= 2000 and
            -10 <= self.sharpe_ratio <= 10 and
            0 <= self.max_drawdown <= 100 and
            0 <= self.win_rate <= 100
        )


class MetricsCalculator:
    """
    回测指标计算器
    
    从净值曲线和交易日志计算各类性能指标。
    
    使用示例:
    ```python
    calc = MetricsCalculator(hold_period=3, backtest_days=120)
    metrics = calc.compute(equity_curve, daily_logs, trade_count, win_count)
    print(f"夏普: {metrics.sharpe_ratio:.2f}")
    ```
    """
    
    def __init__(
        self, 
        hold_period: int = 3, 
        backtest_days: int = 120,
        risk_free_rate: float = 0.02  # 无风险利率 (年化)
    ):
        """
        初始化指标计算器
        
        Args:
            hold_period: 持仓周期 (天)
            backtest_days: 回测天数
            risk_free_rate: 无风险利率 (默认2%)
        """
        self.hold_period = hold_period
        self.backtest_days = backtest_days
        self.risk_free_rate = risk_free_rate
        self.periods_per_year = 252 / hold_period
    
    def compute(
        self,
        equity_curve: List[float],
        daily_logs: List[Dict],
        trade_count: int,
        win_count: int
    ) -> BacktestMetrics:
        """
        计算完整的回测指标
        
        Args:
            equity_curve: 净值曲线列表 [1.0, 1.02, 0.98, ...]
            daily_logs: 每日统计列表 [{'date': ..., 'ret': ..., 'picks': ...}, ...]
            trade_count: 总交易次数
            win_count: 盈利交易次数
            
        Returns:
            BacktestMetrics 数据类实例
        """
        # 防御性检查
        if not daily_logs or len(equity_curve) < 2:
            return self._empty_metrics()
        
        curve = np.array(equity_curve)
        df_log = pd.DataFrame(daily_logs)
        
        if 'ret' not in df_log.columns or df_log['ret'].isna().all():
            return self._empty_metrics()
        
        # 基础收益指标
        total_return = (curve[-1] - 1) * 100
        annual_return = self._annualize_return(curve[-1])
        
        # 风险指标
        max_drawdown = self._compute_max_drawdown(curve)
        sharpe = self._compute_sharpe(df_log['ret'].values)
        sortino = self._compute_sortino(df_log['ret'].values)
        calmar = annual_return / max_drawdown if max_drawdown > 0.01 else 0
        
        # 交易统计
        win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0
        avg_profit = self._compute_avg_profit(df_log['ret'].values, trade_count)
        profit_factor = self._compute_profit_factor(df_log['ret'].values)
        
        return BacktestMetrics(
            total_return=total_return,
            annual_return=annual_return,
            sharpe_ratio=sharpe,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            trade_count=trade_count,
            avg_profit=avg_profit,
            profit_factor=profit_factor,
            final_nav=curve[-1],
            calmar_ratio=calmar,
            sortino_ratio=sortino,
        )
    
    def _annualize_return(self, final_nav: float) -> float:
        """计算年化收益率"""
        if self.backtest_days <= 0 or final_nav <= 0:
            return 0.0
        return (final_nav ** (252 / self.backtest_days) - 1) * 100
    
    def _compute_max_drawdown(self, curve: np.ndarray) -> float:
        """计算最大回撤"""
        max_vals = np.maximum.accumulate(curve)
        drawdowns = (max_vals - curve) / (max_vals + 1e-9)
        return np.max(drawdowns) * 100
    
    def _compute_sharpe(self, returns: np.ndarray) -> float:
        """
        计算夏普比率
        
        Sharpe = (平均收益 - 无风险收益) / 收益标准差 × √(年化因子)
        """
        if len(returns) < 2:
            return 0.0
        
        ret_mean = np.mean(returns)
        ret_std = np.std(returns, ddof=1)
        
        if ret_std < 1e-9:
            return 0.0
        
        # 无风险收益调整 (每期)
        rf_per_period = self.risk_free_rate / self.periods_per_year
        excess_return = ret_mean - rf_per_period
        
        return (excess_return / ret_std) * np.sqrt(self.periods_per_year)
    
    def _compute_sortino(self, returns: np.ndarray) -> float:
        """
        计算索提诺比率 (只考虑下行波动)
        
        Sortino = (平均收益 - 无风险收益) / 下行标准差 × √(年化因子)
        """
        if len(returns) < 2:
            return 0.0
        
        ret_mean = np.mean(returns)
        negative_returns = returns[returns < 0]
        
        if len(negative_returns) == 0:
            return 5.0  # 无负收益，给予高评分
        
        downside_std = np.std(negative_returns, ddof=1)
        
        if downside_std < 1e-9:
            return 0.0
        
        rf_per_period = self.risk_free_rate / self.periods_per_year
        excess_return = ret_mean - rf_per_period
        
        return (excess_return / downside_std) * np.sqrt(self.periods_per_year)
    
    def _compute_avg_profit(self, returns: np.ndarray, trade_count: int) -> float:
        """计算平均单笔收益"""
        if trade_count <= 0:
            return 0.0
        total = np.sum(returns) * 100  # 转换为百分比
        return total / trade_count
    
    def _compute_profit_factor(self, returns: np.ndarray) -> float:
        """
        计算盈亏比
        
        盈亏比 = 总盈利 / |总亏损|
        """
        profits = returns[returns > 0]
        losses = returns[returns < 0]
        
        total_profit = np.sum(profits) if len(profits) > 0 else 0
        total_loss = np.abs(np.sum(losses)) if len(losses) > 0 else 0
        
        if total_loss < 1e-9:
            return 10.0 if total_profit > 0 else 0.0
        
        return total_profit / total_loss
    
    def _empty_metrics(self) -> BacktestMetrics:
        """返回空指标"""
        return BacktestMetrics(
            total_return=0.0,
            annual_return=0.0,
            sharpe_ratio=0.0,
            max_drawdown=0.0,
            win_rate=0.0,
            trade_count=0,
            avg_profit=0.0,
            profit_factor=0.0,
            final_nav=1.0,
            calmar_ratio=0.0,
            sortino_ratio=0.0,
        )


class MetricsValidator:
    """
    指标验证器
    
    检查回测指标是否在合理范围内，防止计算错误。
    """
    
    # 合理范围阈值
    THRESHOLDS = {
        'total_return': (-100, 1000),      # -100% ~ 1000%
        'annual_return': (-100, 2000),     # -100% ~ 2000%
        'sharpe_ratio': (-5, 10),          # -5 ~ 10
        'max_drawdown': (0, 100),          # 0% ~ 100%
        'win_rate': (0, 100),              # 0% ~ 100%
        'profit_factor': (0, 20),          # 0 ~ 20
    }
    
    @classmethod
    def validate(cls, metrics: BacktestMetrics) -> Tuple[bool, List[str]]:
        """
        验证指标合理性
        
        Returns:
            (是否通过, 异常列表)
        """
        errors = []
        
        for field, (min_val, max_val) in cls.THRESHOLDS.items():
            value = getattr(metrics, field, 0)
            if value < min_val or value > max_val:
                errors.append(f"{field}={value:.2f} 超出范围 [{min_val}, {max_val}]")
        
        return len(errors) == 0, errors
    
    @classmethod
    def warn_if_abnormal(cls, metrics: BacktestMetrics) -> None:
        """如果指标异常则打印警告"""
        is_valid, errors = cls.validate(metrics)
        if not is_valid:
            logger.warning(f"[MetricsValidator] 指标异常: {errors}")


def compute_metrics(
    equity_curve: List[float],
    daily_logs: List[Dict],
    trade_count: int,
    win_count: int,
    hold_period: int = 3,
    backtest_days: int = 120
) -> BacktestMetrics:
    """
    便捷函数：计算回测指标
    
    Args:
        equity_curve: 净值曲线
        daily_logs: 每日统计
        trade_count: 交易次数
        win_count: 盈利次数
        hold_period: 持仓周期
        backtest_days: 回测天数
        
    Returns:
        BacktestMetrics 实例
    """
    calc = MetricsCalculator(
        hold_period=hold_period,
        backtest_days=backtest_days
    )
    return calc.compute(equity_curve, daily_logs, trade_count, win_count)
