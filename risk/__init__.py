# -*- coding: utf-8 -*-
"""
Risk 模块 - 统一风险管理与退出规则

将 engine.py 和 simulator.py 中重复的退出逻辑
提取到这里，确保实盘持仓监测和回测行为完全一致。

主要组件:
- ExitRuleEngine: 退出规则引擎
- ExitConfig: 退出规则配置
- ExitResult: 退出检查结果
- ExitType: 退出类型枚举
- check_realtime_exit(): 实时退出检查便捷函数
- simulate_smart_exit(): 回测退出模拟便捷函数

使用示例:
    >>> from momentum.risk import check_realtime_exit, ExitConfig
    >>> result = check_realtime_exit(row, cost_price=10.5)
    >>> print(result.action, result.should_exit)
"""

from .exit_rules import (
    ExitRuleEngine,
    ExitConfig,
    ExitResult,
    ExitType,
    check_realtime_exit,
    simulate_smart_exit,
)

__all__ = [
    'ExitRuleEngine',
    'ExitConfig',
    'ExitResult',
    'ExitType',
    'check_realtime_exit',
    'simulate_smart_exit',
]
