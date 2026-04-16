# -*- coding: utf-8 -*-
"""
Portfolio Report Generator - 持仓诊断报告生成器

生成持仓监控诊断报告。
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Optional
from .formatter import ReportFormatter, pad_str


class PortfolioReportGenerator:
    """
    持仓报告生成器
    
    从 engine._display_portfolio_report() 提取。
    """
    
    def __init__(self, engine):
        """
        初始化报告生成器
        
        Args:
            engine: MomentumEngine 实例 (用于访问市场状态和持仓成本)
        """
        self.engine = engine
    
    def generate(self, df: pd.DataFrame) -> str:
        """
        生成持仓诊断报告
        
        Args:
            df: 持仓数据 DataFrame
            
        Returns:
            报告文本
        """
        from .. import config as cfg
        from ..notify import send_feishu_msg
        
        formatter = ReportFormatter("持仓监控报告", width=180)
        
        # 获取止损止盈参数
        fixed_stop_pct = getattr(cfg, 'FIXED_STOP_PCT', 0.05)
        take_profit_pct = getattr(cfg, 'TAKE_PROFIT_PCT', 0.10)
        bias_profit_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20)
        rsi_danger_zone = getattr(cfg, 'RSI_DANGER_ZONE', 80.0)
        
        # 头部
        now = datetime.now()
        time_str = now.strftime('%Y-%m-%d %H:%M:%S')
        market_phase = self._get_market_phase(now)
        
        header = f"持仓监控报告 | 时间: {time_str} | {market_phase} | 成交额: {self.engine.market_total_amount/1e8:.0f}亿"
        formatter.add_header(header)
        
        # 止盈止损规则说明
        formatter.add_section("止盈止损规则 (优先级从高到低)", [
            f"① 固定止盈: 触及买入价×{(1+take_profit_pct)*100:.0f}% → 立即止盈",
            f"② 固定止损: 跌破买入价×{(1-fixed_stop_pct)*100:.0f}% → 立即清仓",
            f"③ MA5止盈: 收盘跌破MA5 → 趋势结束离场",
            f"④ 乖离率止盈: 偏离MA20超{bias_profit_limit*100:.0f}% → 冲高回落风险，止盈",
            f"⑤ RSI止盈: RSI>{rsi_danger_zone:.0f} → 超买区域，分批止盈",
            "⑥ 破MA20清仓: 收盘跌破MA20 → 趋势反转，清仓",
        ])
        formatter.add_separator()
        
        # 表头
        table_header = (
            f"{pad_str('代码', 8)} {pad_str('买入价', 8)} {pad_str('现价', 8)} "
            f"{pad_str('盈亏%', 8)} {pad_str('止损价', 9)} {pad_str('MA5', 8)} "
            f"{pad_str('乖离%', 7)} {pad_str('RSI', 6)} {pad_str('MA20', 8)} "
            f"{pad_str('止盈信号', 12)} {'操作建议'}"
        )
        formatter.add_line(table_header)
        formatter.add_separator()
        
        # 表格数据
        for _, r in df.sort_values('alpha_score', ascending=False).iterrows():
            line = self._format_portfolio_row(r, cfg)
            formatter.add_line(line)
        
        formatter.add_separator("=")
        formatter.add_line("💡 信号说明: 🚨=立即行动 ⚠️=警惕观察 ✅=持有")
        
        report_text = formatter.build()
        
        # 控制台输出
        print("\n" + "💼" * 15 + " 持仓监控报告 " + "💼" * 15)
        for line in formatter.lines:
            print(line)
        print("█" * 180)
        
        # 飞书通知
        send_feishu_msg("持仓诊断报告", report_text)
        
        return report_text
    
    def _get_market_phase(self, now: datetime) -> str:
        """获取市场阶段描述"""
        hour = now.hour
        if hour < 9 or (hour == 9 and now.minute < 30):
            return "盘前"
        elif hour < 11 or (hour == 11 and now.minute < 30):
            return "上午盘中(成交数据不完整)"
        elif hour < 13:
            return "午间休市(成交为上午数据)"
        elif hour < 15:
            return "下午盘中" if hour < 14 or now.minute < 30 else "尾盘(数据接近完整)"
        else:
            return "盘后(全天完整数据)"
    
    def _format_portfolio_row(self, r: pd.Series, cfg) -> str:
        """格式化持仓行"""
        fixed_stop_pct = getattr(cfg, 'FIXED_STOP_PCT', 0.05)
        take_profit_pct = getattr(cfg, 'TAKE_PROFIT_PCT', 0.10)
        bias_profit_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20)
        rsi_danger_zone = getattr(cfg, 'RSI_DANGER_ZONE', 80.0)
        
        code = str(r['code'])
        current_price = r['close']
        ma5 = r.get('ma5', current_price * 0.98)
        ma20 = r.get('ma20', current_price * 0.95)
        rsi = r.get('rsi', 50)
        
        # 乖离率
        bias_20 = ((current_price / ma20) - 1) if ma20 > 0 else 0
        bias_str = f"{bias_20*100:+.1f}%"
        
        # 成本与盈亏
        cost_price = self.engine.holding_costs.get(code, 0)
        if cost_price > 0:
            cost_str = f"{cost_price:.2f}"
            pnl_pct = ((current_price / cost_price) - 1) * 100
            pnl_str = f"{pnl_pct:+.1f}%"
            stop_price = cost_price * (1 - fixed_stop_pct)
            stop_str = f"{stop_price:.2f}"
            take_profit_price = cost_price * (1 + take_profit_pct)
        else:
            cost_str = "-"
            pnl_str = "-"
            stop_str = "-"
            take_profit_price = 0
            stop_price = 0
        
        # 止盈信号
        profit_signals = []
        if cost_price > 0 and current_price >= take_profit_price:
            profit_signals.append("止盈💰")
        if cost_price > 0 and current_price <= stop_price:
            profit_signals.append("止损🚨")
        if current_price < ma5:
            profit_signals.append("破MA5🚨")
        if bias_20 >= bias_profit_limit:
            profit_signals.append("乖离⚠️")
        if rsi >= rsi_danger_zone:
            profit_signals.append("RSI⚠️")
        if current_price < ma20:
            profit_signals.append("破MA20🚨")
        
        signal_str = " ".join(profit_signals[:2]) if profit_signals else "持有✅"
        action = r.get('action', '✅ [持有]')
        
        return (
            f"{pad_str(code, 8)} {pad_str(cost_str, 8)} {current_price:<8.2f} "
            f"{pad_str(pnl_str, 8)} {pad_str(stop_str, 9)} {ma5:<8.2f} "
            f"{pad_str(bias_str, 7)} {rsi:<6.1f} {ma20:<8.2f} "
            f"{pad_str(signal_str, 12)} {action}"
        )
