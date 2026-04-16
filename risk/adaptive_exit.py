# -*- coding: utf-8 -*-
"""
自适应止损规则模块

根据市场状态和个股特征动态调整止盈止损参数，
提高策略在不同市场环境下的适应性。
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger('momentum.risk')


@dataclass
class AdaptiveExitParams:
    """自适应退出参数"""
    stop_loss_pct: float      # 止损百分比
    take_profit_pct: float    # 止盈百分比
    atr_multiplier: float     # ATR倍数
    bias_limit: float         # 乖离率止盈阈值
    rsi_exit: float           # RSI退出阈值
    reason: str               # 调整原因


class AdaptiveExitEngine:
    """
    自适应退出规则引擎
    
    根据以下因素动态调整止盈止损参数：
    1. 个股波动率 (ATR)
    2. 当前RSI水平
    3. 乖离率状态
    4. 市场整体环境
    
    使用示例:
    ```python
    engine = AdaptiveExitEngine()
    
    params = engine.get_adaptive_params(
        atr_pct=2.5,
        rsi=72,
        bias=0.12,
        market_condition='bullish'
    )
    
    print(f"止损: {params.stop_loss_pct:.1%}")
    print(f"止盈: {params.take_profit_pct:.1%}")
    ```
    """
    
    # 基准参数
    BASE_STOP_LOSS = 0.05       # 基准止损 5%
    BASE_TAKE_PROFIT = 0.10     # 基准止盈 10%
    BASE_ATR_MULT = 1.2         # 基准ATR倍数
    BASE_BIAS_LIMIT = 0.20      # 基准乖离率上限
    BASE_RSI_EXIT = 80.0        # 基准RSI退出位
    
    def __init__(self):
        """初始化自适应引擎"""
        self.adjustment_log: list = []  # 调整记录
    
    def get_adaptive_params(
        self,
        atr_pct: float = 1.5,
        rsi: float = 50.0,
        bias: float = 0.0,
        market_condition: str = 'normal',
        entry_price: Optional[float] = None,
        current_price: Optional[float] = None,
    ) -> AdaptiveExitParams:
        """
        获取自适应退出参数
        
        Args:
            atr_pct: ATR占价格的百分比 (如 2.0 表示 2%)
            rsi: 当前RSI值
            bias: 当前乖离率
            market_condition: 市场状态 (bullish/bearish/normal/volatile)
            entry_price: 买入价格 (可选)
            current_price: 当前价格 (可选)
            
        Returns:
            AdaptiveExitParams 实例
        """
        stop_loss = self.BASE_STOP_LOSS
        take_profit = self.BASE_TAKE_PROFIT
        atr_mult = self.BASE_ATR_MULT
        bias_limit = self.BASE_BIAS_LIMIT
        rsi_exit = self.BASE_RSI_EXIT
        
        reasons = []
        
        # ========== 1. 波动率调整 ==========
        if atr_pct > 3.0:
            # 高波动：放宽止损，收紧止盈
            stop_loss = 0.08
            take_profit = 0.08
            atr_mult = 1.5
            reasons.append(f"高波动(ATR={atr_pct:.1f}%)")
        elif atr_pct > 2.0:
            # 中等波动
            stop_loss = 0.06
            take_profit = 0.09
            atr_mult = 1.3
            reasons.append(f"中波动(ATR={atr_pct:.1f}%)")
        elif atr_pct < 0.8:
            # 低波动：收紧止损
            stop_loss = 0.03
            take_profit = 0.06
            atr_mult = 1.0
            reasons.append(f"低波动(ATR={atr_pct:.1f}%)")
        
        # ========== 2. RSI调整 ==========
        if rsi > 80:
            # 极度超买：更快止盈
            take_profit = min(take_profit, 0.06)
            rsi_exit = 82
            reasons.append(f"超买(RSI={rsi:.0f})")
        elif rsi > 70:
            # 超买区：适度收紧
            take_profit = min(take_profit, 0.08)
            rsi_exit = 80
            reasons.append(f"偏高(RSI={rsi:.0f})")
        elif rsi < 30:
            # 超卖区：放宽止损等反弹
            stop_loss = max(stop_loss, 0.07)
            reasons.append(f"超卖(RSI={rsi:.0f})")
        
        # ========== 3. 乖离率调整 ==========
        if bias > 0.15:
            # 高乖离：更快止盈
            bias_limit = 0.18
            take_profit = min(take_profit, 0.07)
            reasons.append(f"高乖离({bias:.1%})")
        elif bias < -0.10:
            # 负乖离（回调中）：放宽止损
            stop_loss = max(stop_loss, 0.07)
            reasons.append(f"回调中({bias:.1%})")
        
        # ========== 4. 市场环境调整 ==========
        if market_condition == 'bullish':
            # 牛市：更激进的止盈
            take_profit = take_profit * 1.3
            stop_loss = stop_loss * 1.1  # 放宽止损
            reasons.append("牛市环境")
        elif market_condition == 'bearish':
            # 熊市：更保守
            take_profit = take_profit * 0.7
            stop_loss = stop_loss * 0.8  # 更紧止损
            reasons.append("熊市环境")
        elif market_condition == 'volatile':
            # 震荡市：平衡策略
            stop_loss = max(stop_loss, 0.06)
            take_profit = min(take_profit, 0.08)
            reasons.append("震荡市环境")
        
        # ========== 5. 已有浮盈调整 ==========
        if entry_price and current_price:
            pnl_pct = (current_price - entry_price) / entry_price
            
            if pnl_pct > 0.05:
                # 已有5%浮盈：移动止损至成本线
                stop_loss = max(0.0, -pnl_pct + 0.02)  # 保护2%利润
                reasons.append(f"浮盈{pnl_pct:.1%}保护")
            elif pnl_pct < -0.03:
                # 已有3%浮亏：收紧止损
                stop_loss = min(stop_loss, 0.04)
                reasons.append(f"浮亏{pnl_pct:.1%}保护")
        
        # 边界检查
        stop_loss = max(0.02, min(0.15, stop_loss))
        take_profit = max(0.03, min(0.25, take_profit))
        
        reason_str = " | ".join(reasons) if reasons else "默认参数"
        
        return AdaptiveExitParams(
            stop_loss_pct=stop_loss,
            take_profit_pct=take_profit,
            atr_multiplier=atr_mult,
            bias_limit=bias_limit,
            rsi_exit=rsi_exit,
            reason=reason_str,
        )
    
    def simulate_adaptive_exit(
        self,
        entry_price: float,
        df: pd.DataFrame,
        entry_idx: int,
        hold_period: int,
        slippage: float = 0.008,
    ) -> Tuple[float, str, int, str]:
        """
        使用自适应参数模拟退出
        
        Args:
            entry_price: 买入价格
            df: K线数据
            entry_idx: 买入日索引
            hold_period: 最大持仓天数
            slippage: 滑点
            
        Returns:
            (收益率, 退出原因, 实际持仓天数, 退出日期)
        """
        df = df.copy()
        
        # 预计算指标
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['atr'] = self._calculate_atr(df)
        
        for i in range(1, hold_period + 1):
            curr_idx = entry_idx + i
            if curr_idx >= len(df):
                break
            
            row = df.iloc[curr_idx]
            prev_row = df.iloc[curr_idx - 1] if curr_idx > 0 else row
            
            # 计算当前状态
            current_price = row['close']
            atr_pct = (row['atr'] / current_price) * 100 if current_price > 0 else 1.5
            rsi = self._calculate_rsi_single(df['close'].iloc[:curr_idx + 1])
            bias = (current_price / row['ma20'] - 1) if row['ma20'] > 0 else 0
            
            # 获取自适应参数
            params = self.get_adaptive_params(
                atr_pct=atr_pct,
                rsi=rsi,
                bias=bias,
                entry_price=entry_price,
                current_price=current_price,
            )
            
            exit_date = self._format_date(row.get('trade_date'))
            
            stop_price = entry_price * (1 - params.stop_loss_pct)
            profit_price = entry_price * (1 + params.take_profit_pct)
            
            # 检查止盈
            if row['high'] >= profit_price:
                fwd_ret = (profit_price / entry_price) - 1 - slippage
                return fwd_ret, f"Adaptive_TP({params.stop_loss_pct:.0%})", i, exit_date
            
            # 检查止损
            if row['low'] <= stop_price:
                fwd_ret = (stop_price / entry_price) - 1 - slippage
                return fwd_ret, f"Adaptive_SL({params.stop_loss_pct:.0%})", i, exit_date
            
            # 检查RSI退出
            if rsi >= params.rsi_exit:
                fwd_ret = (current_price / entry_price) - 1 - slippage
                return fwd_ret, f"Adaptive_RSI({rsi:.0f})", i, exit_date
            
            # 检查乖离率退出
            if bias >= params.bias_limit:
                fwd_ret = (current_price / entry_price) - 1 - slippage
                return fwd_ret, f"Adaptive_Bias({bias:.0%})", i, exit_date
            
            # 检查MA5止盈
            if current_price < row['ma5']:
                fwd_ret = (current_price / entry_price) - 1 - slippage
                return fwd_ret, "Adaptive_MA5", i, exit_date
        
        # 期满离场
        end_idx = min(entry_idx + hold_period, len(df) - 1)
        exit_price = df['close'].iloc[end_idx]
        exit_date = self._format_date(df['trade_date'].iloc[end_idx])
        fwd_ret = (exit_price / entry_price) - 1 - slippage
        
        return fwd_ret, "Time_Exit", hold_period, exit_date
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算ATR"""
        high = df['high']
        low = df['low']
        close = df['close'].shift(1)
        
        tr1 = high - low
        tr2 = abs(high - close)
        tr3 = abs(low - close)
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    
    def _calculate_rsi_single(self, prices: pd.Series, period: int = 14) -> float:
        """计算单个RSI值"""
        if len(prices) < period + 1:
            return 50.0
        
        delta = prices.diff()
        gain = delta.where(delta > 0, 0.0).rolling(period).mean().iloc[-1]
        loss = (-delta).where(delta < 0, 0.0).rolling(period).mean().iloc[-1]
        
        if loss == 0:
            return 100.0
        
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def _format_date(self, date) -> str:
        """格式化日期"""
        if hasattr(date, 'strftime'):
            return date.strftime('%Y-%m-%d')
        return str(date)[:10]


def get_adaptive_exit_params(
    atr_pct: float = 1.5,
    rsi: float = 50.0,
    bias: float = 0.0,
    market_condition: str = 'normal'
) -> AdaptiveExitParams:
    """
    便捷函数：获取自适应退出参数
    
    Args:
        atr_pct: ATR占价格百分比
        rsi: RSI值
        bias: 乖离率
        market_condition: 市场状态
        
    Returns:
        自适应参数
    """
    engine = AdaptiveExitEngine()
    return engine.get_adaptive_params(atr_pct, rsi, bias, market_condition)
