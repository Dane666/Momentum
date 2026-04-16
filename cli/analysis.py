# -*- coding: utf-8 -*-
"""
分析报告相关命令

包含:
- run_trade_analysis: 交易原因分析
- show_strategy_rules: 显示策略规则
"""

import os
import logging
from typing import Optional

logger = logging.getLogger('momentum')


def run_trade_analysis(session_id: Optional[str] = None, save_dir: str = "./reports"):
    """详细分析交易原因
    
    分析每笔交易的买入原因、卖出原因、止损设置，
    并检查是否符合动量策略规则。
    
    Args:
        session_id: 会话 ID（可选，默认使用最近一次）
        save_dir: 报告保存目录
    """
    from momentum.data.trade_reason import analyze_trades_from_db
    from momentum.backtest import get_backtest_sessions
    
    logger.info("=" * 60)
    logger.info("Momentum v16 - 交易原因分析")
    logger.info("=" * 60)
    
    # 如果没有指定 session，使用最近一次
    if not session_id:
        sessions = get_backtest_sessions(limit=1)
        if sessions.empty:
            print("\n❌ 没有找到回测记录\n")
            print("请先运行回测: python main.py --mode backtest\n")
            return
        session_id = sessions.iloc[0]['session_id']
        print(f"\n使用最近的回测会话: {session_id}\n")
    
    try:
        # 生成详细分析报告
        report = analyze_trades_from_db(session_id)
        print(report)
        
        # 保存到文件
        os.makedirs(save_dir, exist_ok=True)
        output_file = os.path.join(save_dir, f"trade_analysis_{session_id[:8]}.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\n📁 分析报告已保存: {output_file}\n")
        
    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()


def show_strategy_rules():
    """显示动量策略规则说明"""
    from momentum.data.trade_reason import explain_strategy_rules
    from momentum import config as cfg
    
    logger.info("=" * 60)
    logger.info("Momentum v16 - 策略规则说明")
    logger.info("=" * 60)
    
    print(explain_strategy_rules())
    
    # 同时显示当前配置参数
    print("\n当前配置参数 (config.py):")
    print("-" * 50)
    print(f"  HOLD_PERIOD_DEFAULT = {cfg.HOLD_PERIOD_DEFAULT}")
    print(f"  ATR_STOP_FACTOR     = {cfg.ATR_STOP_FACTOR}")
    print(f"  RSI_DANGER_ZONE     = {cfg.RSI_DANGER_ZONE}")
    print(f"  MIN_SHARPE          = {cfg.MIN_SHARPE}")
    print(f"  MAX_SECTOR_PICKS    = {cfg.MAX_SECTOR_PICKS}")
    print(f"  MAX_TOTAL_PICKS     = {cfg.MAX_TOTAL_PICKS}")
    print(f"  SLIPPAGE            = {cfg.SLIPPAGE}")
    print("-" * 50)
    print()
