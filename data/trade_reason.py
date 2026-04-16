# -*- coding: utf-8 -*-
"""
交易原因解析器
解释每笔交易的买入/卖出逻辑，以及止损设置
"""

from typing import Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class TradeReason:
    """交易原因数据类"""
    # 买入相关
    buy_signals: list          # 买入信号列表
    buy_summary: str           # 买入原因摘要
    
    # 卖出相关
    exit_reason: str           # 退出原因代码
    exit_summary: str          # 退出原因摘要
    
    # 止损设置
    stop_loss_type: str        # 止损类型
    stop_loss_price: float     # 止损价格
    stop_loss_pct: float       # 止损百分比
    
    # 策略合规性
    is_compliant: bool         # 是否符合策略
    compliance_notes: list     # 合规性说明


class TradeReasonAnalyzer:
    """
    交易原因分析器
    
    分析每笔交易的买卖原因，检查是否符合动量策略规则
    """
    
    # 策略参数 (与 config.py 保持一致)
    RSI_DANGER_ZONE = 85.0
    MIN_SHARPE = 0.8
    ATR_STOP_FACTOR = 1.5
    MAX_SECTOR_PICKS = 2
    MAX_TOTAL_PICKS = 5
    
    def __init__(self):
        pass
    
    def analyze_buy_reason(
        self,
        alpha_score: float,
        mom_5: float,
        mom_20: float,
        sharpe: float,
        rsi: float,
        bias_20: float,
        close: float,
        ma20: float,
        sector: str = '',
        sector_count: int = 0,
        rank: int = 0
    ) -> Tuple[list, str, bool, list]:
        """
        分析买入原因
        
        Args:
            alpha_score: Alpha 综合得分
            mom_5: 5日动量
            mom_20: 20日动量
            sharpe: 夏普比率
            rsi: RSI 指标
            bias_20: 20日乖离率
            close: 收盘价
            ma20: 20日均线
            sector: 板块
            sector_count: 同板块已选数量
            rank: Alpha 排名
            
        Returns:
            (signals, summary, is_compliant, notes)
        """
        signals = []
        notes = []
        is_compliant = True
        
        # ============ 动量信号 ============
        if mom_5 > 0.03:
            signals.append(f"📈 5日动量强势 (+{mom_5*100:.1f}%)")
        elif mom_5 > 0:
            signals.append(f"↗️ 5日动量正向 (+{mom_5*100:.1f}%)")
        
        if mom_20 > 0:
            signals.append(f"📊 20日动量正向 (调整后: {mom_20:.2f})")
        
        # ============ 夏普信号 ============
        if sharpe > 2.0:
            signals.append(f"⭐ 夏普极佳 ({sharpe:.2f})")
        elif sharpe > self.MIN_SHARPE:
            signals.append(f"✅ 夏普达标 ({sharpe:.2f} > {self.MIN_SHARPE})")
        else:
            notes.append(f"⚠️ 夏普偏低 ({sharpe:.2f} < {self.MIN_SHARPE})")
            is_compliant = False
        
        # ============ 趋势信号 ============
        if close > ma20:
            pct_above = ((close / ma20) - 1) * 100
            signals.append(f"🔺 站上MA20 (+{pct_above:.1f}%)")
        else:
            notes.append(f"❌ 未站上MA20 (收盘 {close:.2f} < MA20 {ma20:.2f})")
            is_compliant = False
        
        # ============ RSI 信号 ============
        if rsi > self.RSI_DANGER_ZONE:
            notes.append(f"❌ RSI超买警告 ({rsi:.1f} > {self.RSI_DANGER_ZONE})")
            is_compliant = False
        elif rsi > 70:
            signals.append(f"⚠️ RSI偏高 ({rsi:.1f})")
        elif rsi > 50:
            signals.append(f"✅ RSI健康 ({rsi:.1f})")
        else:
            signals.append(f"📉 RSI偏低 ({rsi:.1f})")
        
        # ============ 乖离率 ============
        if bias_20 > 0.15:
            signals.append(f"⚡ 强势突破 (乖离率 {bias_20*100:.1f}%)")
        elif bias_20 > 0.05:
            signals.append(f"↑ 温和上涨 (乖离率 {bias_20*100:.1f}%)")
        
        # ============ Alpha 排名 ============
        if rank <= 3:
            signals.append(f"🏆 Alpha排名第{rank}位 ({alpha_score:.2f})")
        elif rank <= 5:
            signals.append(f"🥈 Alpha排名第{rank}位 ({alpha_score:.2f})")
        else:
            signals.append(f"Alpha得分: {alpha_score:.2f}")
        
        # ============ 板块控制 ============
        if sector_count >= self.MAX_SECTOR_PICKS:
            notes.append(f"⚠️ 同板块({sector})已达上限 ({sector_count}只)")
        
        # 生成摘要
        if is_compliant:
            summary = f"动量策略买入: Alpha得分{alpha_score:.2f}, " \
                      f"5日动量{mom_5*100:+.1f}%, 夏普{sharpe:.2f}, 站上MA20"
        else:
            summary = f"买入条件未完全满足: " + "; ".join(notes)
        
        return signals, summary, is_compliant, notes
    
    def analyze_exit_reason(
        self,
        exit_code: str,
        entry_price: float,
        exit_price: float,
        atr: float,
        ma20: float,
        ma5: float,
        hold_days: int
    ) -> Tuple[str, str, float]:
        """
        分析卖出原因
        
        Args:
            exit_code: 退出代码 (Take_Profit/Stop_Loss/MA5_Exit/Bias_Exit/RSI_Exit/MA20_Exit/Time_Exit)
            entry_price: 买入价格
            exit_price: 卖出价格
            atr: ATR 值
            ma20: 卖出时 MA20
            ma5: 卖出时 MA5
            hold_days: 持仓天数
            
        Returns:
            (summary, detail, stop_price)
        """
        pnl_pct = ((exit_price / entry_price) - 1) * 100
        pnl_emoji = "🟢" if pnl_pct > 0 else "🔴"
        
        if exit_code == "Take_Profit":
            summary = "固定止盈触发"
            detail = f"{pnl_emoji} 日内最高价触及固定止盈位, " \
                     f"持仓{hold_days}天, 收益{pnl_pct:+.2f}%"
            return summary, detail, exit_price

        elif exit_code == "Stop_Loss":
            summary = "固定止损触发"
            detail = f"{pnl_emoji} 日内最低价触及固定止损位, " \
                     f"持仓{hold_days}天, 收益{pnl_pct:+.2f}%"
            return summary, detail, exit_price

        elif exit_code == "MA5_Exit":
            summary = "MA5趋势止盈"
            detail = f"{pnl_emoji} 收盘价跌破5日均线 (收盘 {exit_price:.2f} < MA5 {ma5:.2f}), " \
                     f"持仓{hold_days}天, 收益{pnl_pct:+.2f}%"
            return summary, detail, ma5

        elif exit_code == "Bias_Exit":
            summary = "乖离率止盈"
            detail = f"{pnl_emoji} 偏离MA20过大，触发止盈, 持仓{hold_days}天, 收益{pnl_pct:+.2f}%"
            return summary, detail, exit_price

        elif exit_code == "RSI_Exit":
            summary = "RSI止盈"
            detail = f"{pnl_emoji} RSI超买触发止盈, 持仓{hold_days}天, 收益{pnl_pct:+.2f}%"
            return summary, detail, exit_price

        elif exit_code == "MA20_Exit":
            summary = "MA20破位止损"
            detail = f"{pnl_emoji} 收盘价跌破MA20, 持仓{hold_days}天, 收益{pnl_pct:+.2f}%"
            return summary, detail, ma20
        
        elif exit_code == "Time_Exit":
            summary = f"持仓期满离场"
            detail = f"{pnl_emoji} 持有{hold_days}天到期自动离场, 收益{pnl_pct:+.2f}%"
            return summary, detail, 0.0
        
        else:
            summary = f"未知退出原因: {exit_code}"
            detail = f"持仓{hold_days}天, 收益{pnl_pct:+.2f}%"
            return summary, detail, 0.0
    
    def calculate_stop_loss(
        self,
        entry_price: float,
        ma20: float,
        atr: float
    ) -> Dict:
        """
        计算止损设置
        
        Args:
            entry_price: 买入价格
            ma20: 买入时 MA20
            atr: 买入时 ATR
            
        Returns:
            止损信息字典
        """
        from .. import config as cfg

        # 固定止损
        fixed_stop_pct = getattr(cfg, 'FIXED_STOP_PCT', 0.05)
        fixed_stop_price = entry_price * (1 - fixed_stop_pct)
        fixed_stop_pct_val = ((fixed_stop_price / entry_price) - 1) * 100

        # MA5 趋势止盈 (大约估算)
        ma5_stop_est = entry_price * 0.98  # 约 2% 趋势线

        return {
            'primary_stop': '固定止损',
            'primary_stop_price': fixed_stop_price,
            'primary_stop_pct': fixed_stop_pct_val,
            'secondary_stop': 'MA5趋势止盈',
            'secondary_stop_desc': '收盘跌破5日均线时离场',
            'tertiary_stop': '时间止损',
            'tertiary_stop_desc': f'持仓满{3}天自动离场',
            'formula': f'止损线 = 买入价 × (1 - {fixed_stop_pct:.0%}) = {fixed_stop_price:.2f}',
            'risk_pct': abs(fixed_stop_pct_val) if fixed_stop_pct_val < 0 else 0
        }
    
    def generate_trade_report(
        self,
        trade_data: Dict
    ) -> str:
        """
        生成单笔交易的详细报告
        
        Args:
            trade_data: 交易数据字典
            
        Returns:
            格式化的报告字符串
        """
        code = trade_data.get('code', '')
        name = trade_data.get('name', '')
        buy_price = trade_data.get('buy_price', 0)
        sell_price = trade_data.get('sell_price', 0)
        alpha_score = trade_data.get('alpha_score', 0)
        mom_5 = trade_data.get('mom_5', 0)
        mom_20 = trade_data.get('mom_20', 0)
        sharpe = trade_data.get('sharpe', 0)
        rsi = trade_data.get('rsi', 0)
        bias_20 = trade_data.get('bias_20', 0)
        atr = trade_data.get('atr', 0)
        ma20 = trade_data.get('ma20', buy_price * 0.95)
        ma5 = trade_data.get('ma5', buy_price * 0.98)
        exit_reason = trade_data.get('exit_reason', 'Time_Exit')
        hold_days = trade_data.get('hold_days', 3)
        pnl_pct = trade_data.get('pnl_pct', 0)
        
        # 分析买入原因
        buy_signals, buy_summary, is_compliant, buy_notes = self.analyze_buy_reason(
            alpha_score, mom_5, mom_20, sharpe, rsi, bias_20, buy_price, ma20
        )
        
        # 分析卖出原因
        exit_summary, exit_detail, stop_price = self.analyze_exit_reason(
            exit_reason, buy_price, sell_price, atr, ma20, ma5, hold_days
        )
        
        # 计算止损设置
        stop_info = self.calculate_stop_loss(buy_price, ma20, atr)
        
        # 生成报告
        report = f"""
{'='*60}
📋 交易报告: {code} {name}
{'='*60}

【买入分析】
  买入价格: ¥{buy_price:.2f}
  买入信号:
"""
        for sig in buy_signals:
            report += f"    • {sig}\n"
        
        report += f"""
  买入摘要: {buy_summary}
  策略合规: {'✅ 符合' if is_compliant else '⚠️ 部分不符合'}
"""
        if buy_notes:
            report += "  注意事项:\n"
            for note in buy_notes:
                report += f"    • {note}\n"
        
        report += f"""
【止损设置】
  主要止损: {stop_info['primary_stop']}
    公式: {stop_info['formula']}
    止损价: ¥{stop_info['primary_stop_price']:.2f} ({stop_info['primary_stop_pct']:.1f}%)
  次要止损: {stop_info['secondary_stop']}
    说明: {stop_info['secondary_stop_desc']}
  最终止损: {stop_info['tertiary_stop']}
    说明: {stop_info['tertiary_stop_desc']}

【卖出分析】
  卖出价格: ¥{sell_price:.2f}
  退出原因: {exit_summary}
  详细说明: {exit_detail}
  持仓收益: {pnl_pct:+.2f}%

{'='*60}
"""
        return report


def explain_strategy_rules() -> str:
    """
    返回动量策略规则说明
    """
    from .. import config as cfg
    take_profit_pct = getattr(cfg, 'TAKE_PROFIT_PCT', 0.10) * 100
    fixed_stop_pct = getattr(cfg, 'FIXED_STOP_PCT', 0.05) * 100
    bias_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20) * 100
    rsi_limit = getattr(cfg, 'RSI_DANGER_ZONE', 80.0)

    return f"""
╔══════════════════════════════════════════════════════════════════════╗
║                    📖 Momentum 动量策略规则说明                          ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  【一、买入条件】(必须全部满足)                                           ║
║                                                                      ║
║   1. Alpha得分排名: 按综合得分降序，取前{cfg.MAX_TOTAL_PICKS}名                              ║
║      公式: Alpha = 0.3×动量 + 0.25×夏普 + 0.2×大单 + 0.15×筹码 + 0.3×NLP  ║
║                                                                      ║
║   2. 趋势确认: 收盘价 > 20日均线 (MA20)                                 ║
║      说明: 确保股票处于上升趋势中                                         ║
║                                                                      ║
║   3. 动量验证: 夏普比率 > {cfg.MIN_SHARPE:.1f}                                           ║
║      说明: 确保收益/风险比合理                                           ║
║                                                                      ║
║   4. 超买过滤: RSI < {rsi_limit:.0f}                                                ║
║      说明: 避免追高极度超买的股票                                         ║
║                                                                      ║
║   5. 流动性要求: 成交额 > 2亿元                                          ║
║      说明: 确保足够的流动性                                              ║
║                                                                      ║
║   6. 乖离率过滤: 偏离MA20 < {bias_limit:.0f}%                                      ║
║      说明: 避免过度拉升后的回撤风险                                       ║
║                                                                      ║
║   7. 行业分散: 同行业最多选{cfg.MAX_SECTOR_PICKS}只                                           ║
║      说明: 避免行业集中风险                                              ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  【二、卖出规则】(优先级从高到低)                                         ║
║                                                                      ║
║   1. 固定止盈 (最高优先级)                                               ║
║      触发条件: 日内最高价 ≥ 买入价×(1+{take_profit_pct:.0f}%)                         ║
║      说明: 快进快出，锁定短线利润                                         ║
║                                                                      ║
║   2. 固定止损 (次优先级)                                                 ║
║      触发条件: 日内最低价 ≤ 买入价×(1-{fixed_stop_pct:.0f}%)                         ║
║      说明: 严格控制回撤，防止深度亏损                                     ║
║                                                                      ║
║   3. MA5趋势止盈                                                        ║
║      触发条件: 收盘价 < 5日均线 (MA5)                                    ║
║      说明: 趋势走弱信号，及时止盈保护利润                                   ║
║                                                                      ║
║   4. 乖离率止盈                                                         ║
║      触发条件: 偏离MA20 ≥ {bias_limit:.0f}%                                         ║
║      说明: 冲高回落风险，提前止盈                                        ║
║                                                                      ║
║   5. RSI止盈                                                            ║
║      触发条件: RSI ≥ {rsi_limit:.0f}                                               ║
║      说明: 超买区域，分批止盈                                            ║
║                                                                      ║
║   6. 破MA20清仓                                                         ║
║      触发条件: 收盘价 < 20日均线 (MA20)                                  ║
║      说明: 趋势反转，快速止损                                            ║
║                                                                      ║
║   7. 期满离场 (最低优先级)                                              ║
║      触发条件: 持仓满N天 (默认3天)                                       ║
║      说明: 短线策略，防止资金占用过久                                      ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  【三、止损计算公式】                                                    ║
║                                                                      ║
║   固定止损线 = 买入价 × (1 - {fixed_stop_pct:.0f}%)                                   ║
║                                                                      ║
║   示例: 若 买入价=100, 止损线=95.0                                      ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  【四、关键参数】(可在 config.py 中调整)                                  ║
║                                                                      ║
║   • HOLD_PERIOD_DEFAULT = 3     # 持仓周期(天)                          ║
║   • FIXED_STOP_PCT = {cfg.FIXED_STOP_PCT:.2f}      # 固定止损比例                      ║
║   • TAKE_PROFIT_PCT = {cfg.TAKE_PROFIT_PCT:.2f}    # 固定止盈比例                      ║
║   • BIAS_PROFIT_LIMIT = {cfg.BIAS_PROFIT_LIMIT:.2f}  # 乖离率止盈阈值                  ║
║   • RSI_DANGER_ZONE = {cfg.RSI_DANGER_ZONE:.1f}      # RSI超买阈值                      ║
║   • MIN_SHARPE = {cfg.MIN_SHARPE:.1f}            # 最低夏普比率                           ║
║   • MAX_SECTOR_PICKS = {cfg.MAX_SECTOR_PICKS}        # 同行业最大持仓                         ║
║   • MAX_TOTAL_PICKS = {cfg.MAX_TOTAL_PICKS}         # 总选股数量                            ║
║   • SLIPPAGE = {cfg.SLIPPAGE:.3f}            # 交易滑点                        ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""


def analyze_trades_from_db(session_id: str) -> str:
    """
    从数据库读取交易并生成详细分析报告
    
    Args:
        session_id: 回测会话 ID
        
    Returns:
        完整的交易分析报告
    """
    from .db import get_session_trades, get_backtest_sessions
    import pandas as pd
    
    # 获取会话信息
    sessions = get_backtest_sessions(limit=100)
    session_info = sessions[sessions['session_id'] == session_id]
    
    if session_info.empty:
        return f"未找到会话: {session_id}"
    
    # 获取交易记录
    trades = get_session_trades(session_id)
    if trades.empty:
        return f"会话 {session_id} 无交易记录"
    
    analyzer = TradeReasonAnalyzer()
    
    # 生成报告头
    row = session_info.iloc[0]
    report = f"""
{'█'*70}
📊 回测交易详细分析报告
{'█'*70}

会话ID: {session_id}
回测天数: {row.get('backtest_days', 0)} 天
持仓周期: {row.get('hold_period', 0)} 天
总收益: {row.get('total_return', 0) or 0:.2f}%
胜率: {row.get('win_rate', 0) or 0:.1f}%
夏普: {row.get('sharpe_ratio', 0) or 0:.2f}

"""
    
    # 策略规则说明
    report += explain_strategy_rules()
    
    # 分析每笔交易
    sells = trades[trades['trade_type'] == 'SELL'].copy()
    buys = trades[trades['trade_type'] == 'BUY'].copy()
    
    report += f"\n\n{'='*70}\n"
    report += f"📋 交易明细分析 (共 {len(sells)} 笔)\n"
    report += f"{'='*70}\n"
    
    for i, (_, sell) in enumerate(sells.iterrows(), 1):
        code = sell['code']
        
        # 找到对应的买入记录
        buy_records = buys[(buys['code'] == code) & (buys['trade_date'] <= sell['trade_date'])]
        if buy_records.empty:
            continue
        buy = buy_records.iloc[-1]
        
        # 构造交易数据
        trade_data = {
            'code': code,
            'name': sell['name'],
            'buy_price': buy['price'],
            'sell_price': sell['price'],
            'alpha_score': buy.get('alpha_score', 0) or 0,
            'mom_5': buy.get('mom_5', 0) or 0,
            'mom_20': buy.get('mom_20', 0) or 0,
            'sharpe': buy.get('sharpe', 0) or 0,
            'rsi': buy.get('rsi', 0) or 0,
            'bias_20': buy.get('bias_20', 0) or 0,
            'atr': buy.get('atr', 0) or 0,
            'ma20': buy['price'] * 0.95,  # 估算 (站上MA20买入)
            'ma5': buy['price'] * 0.98,
            'exit_reason': sell['exit_reason'],
            'hold_days': sell.get('hold_days', 3) or 3,
            'pnl_pct': sell.get('pnl_pct', 0) or 0
        }
        
        report += f"\n【第{i}笔】{code} {sell['name']}\n"
        report += f"买入日期: {buy['trade_date']} | 卖出日期: {sell['trade_date']}\n"
        report += analyzer.generate_trade_report(trade_data)
    
    # 汇总统计
    report += f"\n\n{'='*70}\n"
    report += f"📈 汇总统计\n"
    report += f"{'='*70}\n"
    
    exit_counts = sells['exit_reason'].value_counts()
    report += "\n退出原因分布:\n"
    for reason, count in exit_counts.items():
        pct = count / len(sells) * 100
        report += f"  • {reason}: {count}笔 ({pct:.1f}%)\n"
    
    win_trades = len(sells[sells['pnl_pct'] > 0])
    loss_trades = len(sells[sells['pnl_pct'] <= 0])
    report += f"\n盈亏分布:\n"
    report += f"  • 盈利: {win_trades}笔 ({win_trades/len(sells)*100:.1f}%)\n"
    report += f"  • 亏损: {loss_trades}笔 ({loss_trades/len(sells)*100:.1f}%)\n"
    
    # 按退出原因分析胜率
    report += "\n各退出原因胜率:\n"
    for reason in exit_counts.index:
        reason_trades = sells[sells['exit_reason'] == reason]
        reason_wins = len(reason_trades[reason_trades['pnl_pct'] > 0])
        reason_win_rate = reason_wins / len(reason_trades) * 100 if len(reason_trades) > 0 else 0
        avg_ret = reason_trades['pnl_pct'].mean()
        report += f"  • {reason}: 胜率{reason_win_rate:.1f}%, 平均收益{avg_ret:.2f}%\n"
    
    report += f"\n{'█'*70}\n"
    
    return report
