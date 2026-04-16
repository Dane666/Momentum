# -*- coding: utf-8 -*-
"""
Exit Rules - 统一退出规则模块

统一持仓监测(engine)和回测(simulator)的退出逻辑，确保两者行为完全一致。

退出规则优先级 (每日检查，先触发者生效):
1. 固定止盈: 盘中高点触及 买入价 × (1 + TAKE_PROFIT_PCT)
2. 固定止损: 盘中低点触及 买入价 × (1 - FIXED_STOP_PCT)
3. MA5止盈: 收盘跌破 MA5
4. 乖离率止盈: 偏离MA20超 BIAS_PROFIT_LIMIT
5. RSI止盈: RSI ≥ RSI_DANGER_ZONE
6. MA20破位: 收盘跌破 MA20
7. 期满离场: 持仓满 N 天

使用示例:
    >>> from momentum.risk import ExitRuleEngine, check_realtime_exit
    >>> result = check_realtime_exit(row, cost_price=10.5)
    >>> print(result.action, result.reason)
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple, Union
from enum import Enum


class ExitType(Enum):
    """退出类型枚举"""
    HOLD = "hold"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    MA5_EXIT = "ma5_exit"
    BIAS_EXIT = "bias_exit"
    RSI_EXIT = "rsi_exit"
    MA20_EXIT = "ma20_exit"
    TIME_EXIT = "time_exit"
    MOMENTUM_WEAK = "momentum_weak"
    MOMENTUM_RECOVER = "momentum_recover"


@dataclass
class ExitResult:
    """退出检查结果"""
    exit_type: ExitType
    action: str
    reason: str
    should_exit: bool
    exit_price: Optional[float] = None
    priority: int = 99  # 优先级 (数字越小优先级越高)


@dataclass
class ExitConfig:
    """退出规则配置"""
    fixed_stop_pct: float = 0.05     # 固定止损 5%
    take_profit_pct: float = 0.10    # 固定止盈 10%
    bias_profit_limit: float = 0.20  # 乖离率止盈 20%
    rsi_danger_zone: float = 80.0    # RSI超买位 80
    
    @classmethod
    def from_config(cls) -> 'ExitConfig':
        """从全局配置加载参数"""
        try:
            from .. import config as cfg
            return cls(
                fixed_stop_pct=getattr(cfg, 'FIXED_STOP_PCT', 0.05),
                take_profit_pct=getattr(cfg, 'TAKE_PROFIT_PCT', 0.10),
                bias_profit_limit=getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20),
                rsi_danger_zone=getattr(cfg, 'RSI_DANGER_ZONE', 80.0),
            )
        except ImportError:
            return cls()


class ExitRuleEngine:
    """
    退出规则引擎
    
    统一持仓监测和回测的退出逻辑判定。
    
    Attributes:
        config: 退出规则配置
        adaptive: 是否使用自适应止损
    """
    
    def __init__(self, config: Optional[ExitConfig] = None, adaptive: bool = False):
        """
        初始化退出规则引擎
        
        Args:
            config: 退出规则配置，默认从全局配置加载
            adaptive: 是否使用自适应止损模式
        """
        self.config = config or ExitConfig.from_config()
        self.adaptive = adaptive
        self._adaptive_engine = None
        
        if adaptive:
            try:
                from .adaptive_exit import AdaptiveExitEngine
                self._adaptive_engine = AdaptiveExitEngine()
            except ImportError:
                logger.warning("AdaptiveExitEngine not found, using fixed exit rules")
                self.adaptive = False
    
    def check_realtime_exit(
        self,
        close: float,
        high: float,
        low: float,
        ma5: float,
        ma20: float,
        rsi: float,
        cost_price: float,
        alpha_score: float = 0.0,
        alpha_trend: float = 0.0,
    ) -> ExitResult:
        """
        实时退出检查 (持仓监测用)
        
        Args:
            close: 当前收盘价/最新价
            high: 当日最高价
            low: 当日最低价
            ma5: 5日均线
            ma20: 20日均线
            rsi: RSI指标
            cost_price: 买入成本价
            alpha_score: Alpha得分 (可选)
            alpha_trend: Alpha趋势 (可选)
            
        Returns:
            ExitResult: 退出检查结果
        """
        cfg = self.config
        
        # 计算止盈止损价位
        take_profit_price = cost_price * (1 + cfg.take_profit_pct) if cost_price > 0 else None
        stop_price = cost_price * (1 - cfg.fixed_stop_pct) if cost_price > 0 else None
        
        # 计算乖离率
        bias_20 = (close / ma20) - 1 if ma20 > 0 else 0
        
        # ① 固定止盈
        if take_profit_price and close >= take_profit_price:
            return ExitResult(
                exit_type=ExitType.TAKE_PROFIT,
                action="💰 [止盈] 触发固定止盈",
                reason="Take_Profit",
                should_exit=True,
                exit_price=take_profit_price,
                priority=1
            )
        
        # ② 固定止损
        if stop_price and close <= stop_price:
            return ExitResult(
                exit_type=ExitType.STOP_LOSS,
                action="🚨 [止损] 跌破固定止损",
                reason="Stop_Loss",
                should_exit=True,
                exit_price=stop_price,
                priority=2
            )
        
        # ③ MA5止盈
        if ma5 > 0 and close < ma5:
            return ExitResult(
                exit_type=ExitType.MA5_EXIT,
                action="📉 [减仓] 跌破MA5",
                reason="MA5_Exit",
                should_exit=True,
                exit_price=close,
                priority=3
            )
        
        # ④ 乖离率止盈
        if bias_20 >= cfg.bias_profit_limit:
            return ExitResult(
                exit_type=ExitType.BIAS_EXIT,
                action="💰 [减仓] 乖离率过高",
                reason="Bias_Exit",
                should_exit=True,
                exit_price=close,
                priority=4
            )
        
        # ⑤ RSI止盈
        if rsi >= cfg.rsi_danger_zone:
            return ExitResult(
                exit_type=ExitType.RSI_EXIT,
                action="💰 [减仓] RSI超买",
                reason="RSI_Exit",
                should_exit=True,
                exit_price=close,
                priority=5
            )
        
        # ⑥ MA20破位
        if ma20 > 0 and close < ma20:
            return ExitResult(
                exit_type=ExitType.MA20_EXIT,
                action="🚨 [清仓] 趋势反转破位",
                reason="MA20_Exit",
                should_exit=True,
                exit_price=close,
                priority=6
            )
        
        # 动能辅助判断 (未触发风控时)
        if alpha_score < 0 and alpha_trend < 0:
            return ExitResult(
                exit_type=ExitType.MOMENTUM_WEAK,
                action="🔄 [换仓] 动能持续衰竭",
                reason="Momentum_Weak",
                should_exit=False,  # 仅建议，不强制
                priority=10
            )
        
        if alpha_score < 0 and alpha_trend > 0:
            return ExitResult(
                exit_type=ExitType.MOMENTUM_RECOVER,
                action="⏳ [观察] 强度底部回升",
                reason="Momentum_Recover",
                should_exit=False,
                priority=11
            )
        
        # 正常持有
        return ExitResult(
            exit_type=ExitType.HOLD,
            action="✅ [持有] 动量稳健",
            reason="Hold",
            should_exit=False,
            priority=99
        )
    
    def simulate_exit(
        self,
        entry_price: float,
        df: pd.DataFrame,
        entry_idx: int,
        hold_period: int,
        slippage: float = 0.008,
    ) -> Tuple[float, str, int, str]:
        """
        回测模式的退出模拟
        
        Args:
            entry_price: 买入价格
            df: 完整K线数据 (需包含 close, high, low, trade_date)
            entry_idx: 买入日在df中的索引
            hold_period: 最大持仓天数
            slippage: 滑点
            
        Returns:
            (收益率, 退出原因, 实际持仓天数, 退出日期)
        """
        # 自适应模式：使用 AdaptiveExitEngine
        if self.adaptive and self._adaptive_engine is not None:
            return self._adaptive_engine.simulate_adaptive_exit(
                entry_price=entry_price,
                df=df,
                entry_idx=entry_idx,
                hold_period=hold_period,
                slippage=slippage,
            )
        
        cfg = self.config
        
        stop_loss_price = entry_price * (1 - cfg.fixed_stop_pct)
        take_profit_price = entry_price * (1 + cfg.take_profit_pct)
        
        # 预计算均线
        df = df.copy()
        df['ma5_hist'] = df['close'].rolling(5).mean()
        df['ma20_hist'] = df['close'].rolling(20).mean()
        
        for i in range(1, hold_period + 1):
            curr_idx = entry_idx + i
            if curr_idx >= len(df):
                break
            
            day_high = df['high'].iloc[curr_idx]
            day_low = df['low'].iloc[curr_idx]
            day_close = df['close'].iloc[curr_idx]
            day_ma5 = df['ma5_hist'].iloc[curr_idx]
            day_ma20 = df['ma20_hist'].iloc[curr_idx]
            exit_date = df['trade_date'].iloc[curr_idx]
            
            # 转换日期格式
            if hasattr(exit_date, 'strftime'):
                exit_date = exit_date.strftime('%Y-%m-%d')
            else:
                exit_date = str(exit_date)[:10]
            
            # ① 固定止盈 (盘中高点触及)
            if day_high >= take_profit_price:
                fwd_ret = (take_profit_price / entry_price) - 1 - slippage
                return fwd_ret, "Take_Profit", i, exit_date
            
            # ② 固定止损 (盘中低点触及)
            if day_low <= stop_loss_price:
                fwd_ret = (stop_loss_price / entry_price) - 1 - slippage
                return fwd_ret, "Stop_Loss", i, exit_date
            
            # ③ MA5 趋势止盈
            if pd.notna(day_ma5) and day_close < day_ma5:
                fwd_ret = (day_close / entry_price) - 1 - slippage
                return fwd_ret, "MA5_Exit", i, exit_date
            
            # ④ 乖离率止盈
            if pd.notna(day_ma20) and day_ma20 > 0:
                bias_20 = (day_close / day_ma20) - 1
                if bias_20 >= cfg.bias_profit_limit:
                    fwd_ret = (day_close / entry_price) - 1 - slippage
                    return fwd_ret, "Bias_Exit", i, exit_date
            
            # ⑤ RSI 止盈
            rsi_val = self._calculate_rsi(df['close'].iloc[:curr_idx + 1])
            if rsi_val >= cfg.rsi_danger_zone:
                fwd_ret = (day_close / entry_price) - 1 - slippage
                return fwd_ret, "RSI_Exit", i, exit_date
            
            # ⑥ 破 MA20 清仓
            if pd.notna(day_ma20) and day_close < day_ma20:
                fwd_ret = (day_close / entry_price) - 1 - slippage
                return fwd_ret, "MA20_Exit", i, exit_date
        
        # ⑦ 期满自动离场
        end_idx = min(entry_idx + hold_period, len(df) - 1)
        exit_date = df['trade_date'].iloc[end_idx]
        if hasattr(exit_date, 'strftime'):
            exit_date = exit_date.strftime('%Y-%m-%d')
        else:
            exit_date = str(exit_date)[:10]
        
        fwd_ret = (df['close'].iloc[end_idx] / entry_price) - 1 - slippage
        actual_days = end_idx - entry_idx
        return fwd_ret, "Time_Exit", actual_days, exit_date
    
    @staticmethod
    def _calculate_rsi(prices: pd.Series, period: int = 14) -> float:
        """计算 RSI 指标"""
        if len(prices) < period + 1:
            return 50.0
        
        delta = prices.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        
        avg_gain = gain.rolling(period).mean().iloc[-1]
        avg_loss = loss.rolling(period).mean().iloc[-1]
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))


# ==================== 便捷函数 ====================

def check_realtime_exit(
    row: pd.Series,
    cost_price: float,
    config: Optional[ExitConfig] = None,
) -> ExitResult:
    """
    实时退出检查 (便捷函数)
    
    Args:
        row: 包含 close, high, low, ma5, ma20, rsi, alpha_score, alpha_trend 的 Series
        cost_price: 买入成本价
        config: 退出配置 (可选)
        
    Returns:
        ExitResult: 退出检查结果
    """
    engine = ExitRuleEngine(config)
    
    return engine.check_realtime_exit(
        close=row.get('close', 0),
        high=row.get('high', row.get('close', 0)),
        low=row.get('low', row.get('close', 0)),
        ma5=row.get('ma5', 0),
        ma20=row.get('ma20', 0),
        rsi=row.get('rsi', 50),
        cost_price=cost_price,
        alpha_score=row.get('alpha_score', 0),
        alpha_trend=row.get('alpha_trend', 0),
    )


def simulate_smart_exit(
    entry_price: float,
    df: pd.DataFrame,
    entry_idx: int,
    hold_period: int,
    slippage: float = 0.008,
    config: Optional[ExitConfig] = None,
) -> Tuple[float, str, int, str]:
    """
    回测退出模拟 (便捷函数)
    
    Args:
        entry_price: 买入价格
        df: 完整K线数据
        entry_idx: 买入日索引
        hold_period: 最大持仓天数
        slippage: 滑点
        config: 退出配置 (可选)
        
    Returns:
        (收益率, 退出原因, 实际持仓天数, 退出日期)
    """
    engine = ExitRuleEngine(config)
    return engine.simulate_exit(entry_price, df, entry_idx, hold_period, slippage)
