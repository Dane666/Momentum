# -*- coding: utf-8 -*-
"""
Report Formatter - 报告格式化工具

提供通用的字符串格式化、对齐、表格生成功能。
"""

from typing import List, Dict, Any
import pandas as pd


def pad_str(s: str, width: int, align: str = '<') -> str:
    """
    处理包含中文字符的字符串填充对齐
    
    Args:
        s: 原字符串
        width: 目标显示宽度
        align: 对齐方式 ('<'左对齐, '>'右对齐, '^'居中)
        
    Returns:
        填充后的字符串
    """
    # 计算显示宽度：中文字符算2，其他算1
    display_width = 0
    for char in s:
        if ord(char) > 127:
            display_width += 2
        else:
            display_width += 1
    
    padding = max(0, width - display_width)
    
    if align == '<':
        return s + ' ' * padding
    elif align == '>':
        return ' ' * padding + s
    else:  # center
        left = padding // 2
        right = padding - left
        return ' ' * left + s + ' ' * right


class ReportFormatter:
    """
    报告格式化器
    
    提供统一的报告生成接口。
    """
    
    def __init__(self, title: str = "", width: int = 130):
        """
        初始化格式化器
        
        Args:
            title: 报告标题
            width: 报告宽度
        """
        self.title = title
        self.width = width
        self.lines: List[str] = []
    
    def add_header(self, header: str) -> 'ReportFormatter':
        """添加标题行"""
        self.lines.append(header)
        self.lines.append("-" * self.width)
        return self
    
    def add_separator(self, char: str = "-") -> 'ReportFormatter':
        """添加分隔线"""
        self.lines.append(char * self.width)
        return self
    
    def add_line(self, line: str) -> 'ReportFormatter':
        """添加一行内容"""
        self.lines.append(line)
        return self
    
    def add_empty_line(self) -> 'ReportFormatter':
        """添加空行"""
        self.lines.append("")
        return self
    
    def add_section(self, title: str, content: List[str]) -> 'ReportFormatter':
        """添加一个章节"""
        self.lines.append(f"📋 {title}:")
        for line in content:
            self.lines.append(f"   {line}")
        return self
    
    def add_funnel(self, stats: Dict[str, Any]) -> 'ReportFormatter':
        """添加漏斗统计"""
        self.lines.append("🔬 选股漏斗:")
        funnel_parts = []
        for key, val in stats.items():
            if key.startswith('_'):
                continue
            if isinstance(val, int):
                funnel_parts.append(f"{key}({val})")
            else:
                funnel_parts.append(f"{key}:{val}")
        self.lines.append("   " + " → ".join(funnel_parts))
        self.lines.append("-" * self.width)
        return self
    
    def add_table_header(self, columns: List[tuple]) -> 'ReportFormatter':
        """
        添加表头
        
        Args:
            columns: [(列名, 宽度), ...] 列表
        """
        header = " ".join(pad_str(col[0], col[1]) for col in columns)
        self.lines.append(header)
        return self
    
    def add_table_row(self, values: List[tuple]) -> 'ReportFormatter':
        """
        添加表格行
        
        Args:
            values: [(值, 宽度), ...] 列表
        """
        row = " ".join(pad_str(str(v[0]), v[1]) for v in values)
        self.lines.append(row)
        return self
    
    def build(self) -> str:
        """生成报告文本"""
        return "\n".join(self.lines)
    
    def print_report(self, emoji: str = "📊") -> str:
        """打印报告到控制台并返回文本"""
        report_text = self.build()
        
        print("\n" + emoji * 15 + f" {self.title} " + emoji * 15)
        for line in self.lines:
            print(line)
        print("█" * self.width)
        
        return report_text


def format_pct(value: float, with_sign: bool = True) -> str:
    """格式化百分比"""
    if with_sign:
        return f"{value:+.1f}%"
    return f"{value:.1f}%"


def format_price(value: float) -> str:
    """格式化价格"""
    return f"{value:.2f}"


def format_amount(value: float) -> str:
    """格式化金额 (转为亿)"""
    return f"{value/1e8:.0f}亿"
