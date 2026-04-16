# -*- coding: utf-8 -*-
"""
Scan Report Generator - 选股报告生成器

生成市场扫描选股报告。
"""

import pandas as pd
from typing import Dict, Any, Optional
from .formatter import ReportFormatter, pad_str


class ScanReportGenerator:
    """
    选股报告生成器
    
    从 engine._display_report() 提取。
    """
    
    def __init__(self, engine):
        """
        初始化报告生成器
        
        Args:
            engine: MomentumEngine 实例 (用于访问市场状态)
        """
        self.engine = engine
    
    def generate(
        self, 
        df: pd.DataFrame, 
        elapsed: float,
        filter_stats: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        生成选股报告
        
        Args:
            df: 选股结果 DataFrame
            elapsed: 耗时 (秒)
            filter_stats: 过滤统计信息
            
        Returns:
            报告文本
        """
        from .. import config as cfg
        from ..notify import send_feishu_msg
        
        formatter = ReportFormatter("选股报告", width=130)
        
        # 头部
        header = (
            f"Momentum v16 [工程化版] | 成交额: {self.engine.market_total_amount/1e8:.0f}亿 | "
            f"情绪: {self.engine.streak_emotion} | 外资: {self.engine.connect_trend} | "
            f"仓位系数: {self.engine.position_multiplier:.1f}x"
        )
        formatter.add_header(header)
        
        # 过滤漏斗
        if filter_stats:
            formatter.add_funnel(filter_stats)
        
        if df.empty:
            self._add_empty_result(formatter, cfg)
        else:
            self._add_stock_table(formatter, df, cfg)
        
        # 策略说明
        formatter.add_separator("=")
        formatter.add_line(f"耗时: {elapsed:.1f}s")
        formatter.add_line("💡 标签说明: [回测买入]=符合回测条件 | [观察]=不符合回测但有潜力")
        formatter.add_line("💡 止损位=买入价×0.95 (固定5%止损) | MA5止盈=跌破5日均线时止盈")
        
        report_text = formatter.build()
        
        # 控制台输出
        print("\n" + "🎯" * 15 + " 选股报告 " + "🎯" * 15)
        for line in formatter.lines:
            print(line)
        print("█" * 130)
        
        # 飞书通知
        send_feishu_msg("Momentum选股", report_text)
        
        return report_text
    
    def _add_empty_result(self, formatter: ReportFormatter, cfg) -> None:
        """添加无结果说明"""
        formatter.add_line("今日无推荐标的")
        formatter.add_empty_line()
        formatter.add_section("可能原因", [
            f"• 涨幅范围: {cfg.MIN_CHANGE_PCT}% ~ {cfg.MAX_CHANGE_PCT}% 内股票数量不足",
            f"• 量比要求: ≥{cfg.MIN_VOL_RATIO} (刚开盘数据可能不准)",
            f"• RSI限制: <{cfg.RSI_DANGER_ZONE} (避免追高)",
            f"• 夏普要求: >{cfg.MIN_SHARPE}",
        ])
    
    def _add_stock_table(self, formatter: ReportFormatter, df: pd.DataFrame, cfg) -> None:
        """添加股票表格"""
        # 计算仓位建议
        initial_capital = getattr(cfg, 'INITIAL_CAPITAL', 100000.0)
        fixed_stop_pct = getattr(cfg, 'FIXED_STOP_PCT', 0.05)
        
        # 符合回测条件的数量
        bt_qualified = [r for _, r in df.iterrows() if self._check_backtest_criteria(r, cfg)[0]]
        num_qualified = len(bt_qualified)
        position_per_stock = initial_capital / num_qualified if num_qualified > 0 else 0
        
        # 表头
        columns = [
            ('代码', 8), ('名称', 10), ('现价', 8), 
            ('MA5止盈', 9), ('固定止损', 10), ('建议股数', 10),
            ('仓位', 10), ('Alpha', 7), ('状态', 20)
        ]
        formatter.add_table_header(columns)
        
        # 表格内容
        for _, r in df.iterrows():
            is_qualified, _ = self._check_backtest_criteria(r, cfg)
            
            close = r['close']
            ma5 = r['ma5']
            stop_price = close * (1 - fixed_stop_pct)
            
            if is_qualified and position_per_stock > 0:
                shares = int(position_per_stock / close / 100) * 100
                position_str = f"{position_per_stock:.0f}元"
            else:
                shares = 0
                position_str = "观察"
            
            values = [
                (r['code'], 8),
                (r['name'][:5], 10),
                (f"{close:.2f}", 8),
                (f"{ma5:.2f}", 9),
                (f"{stop_price:.2f}", 10),
                (str(shares) if shares > 0 else "-", 10),
                (position_str, 10),
                (f"{r['alpha_score']:.2f}", 7),
                (r.get('action', ''), 20),
            ]
            formatter.add_table_row(values)
    
    def _check_backtest_criteria(self, row, cfg) -> tuple:
        """检查回测买入标准"""
        reasons = []
        
        rsi_limit = getattr(cfg, 'RSI_DANGER_ZONE', 80.0)
        if row.get('rsi', 0) > rsi_limit:
            reasons.append(f"RSI>{rsi_limit:.0f}")
        
        if row.get('close', 0) <= row.get('ma20', float('inf')):
            reasons.append("<MA20")
        
        if row.get('sharpe_t', 0) <= cfg.MIN_SHARPE:
            reasons.append(f"Sharpe<{cfg.MIN_SHARPE}")
        
        bias_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20)
        ma20 = row.get('ma20', 0)
        close = row.get('close', 0)
        if ma20 > 0:
            bias_20 = (close / ma20) - 1
            if bias_20 >= bias_limit:
                reasons.append(f"乖离>{bias_limit*100:.0f}%")
        
        return len(reasons) == 0, reasons
