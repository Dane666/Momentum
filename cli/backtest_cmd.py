# -*- coding: utf-8 -*-
"""
回测相关命令

包含:
- run_backtest: 执行回测并生成报告
- show_backtest_history: 显示回测历史
- show_session_detail: 显示会话详情
- run_visualize: 可视化回测结果
"""

import os
import logging
from typing import Optional, List

logger = logging.getLogger('momentum')


def run_backtest(
    days: int = 250,
    periods: Optional[List[int]] = None,
    record_trades: bool = True,
    auto_report: bool = True,
    save_dir: str = "./reports"
):
    """执行回测并自动生成完整报告
    
    Args:
        days: 回测天数
        periods: 持仓周期列表
        record_trades: 是否记录交易到数据库 (用于可视化)
        auto_report: 是否自动生成完整报告（含可视化、交易分析、操作指南）
        save_dir: 报告保存目录
        
    Returns:
        回测结果
    """
    from momentum.backtest import run_sensitivity_analysis, get_backtest_sessions
    from momentum.data import clear_all_backtest_data
    from momentum import config as cfg

    logger.info("=" * 60)
    logger.info("Momentum v16 - 回测模式")
    logger.info("=" * 60)
    
    # 每次回测前清空历史数据，确保干净的回测环境
    print(f"🗑️  清空历史回测数据...")
    clear_all_backtest_data()
    
    # 显示初始资金设置
    initial_capital = getattr(cfg, 'INITIAL_CAPITAL', 100000.0)
    print(f"💰 初始资金: ¥{initial_capital:,.0f}")

    if periods is None:
        periods = [3, 4]

    # 使用 window_shift=1 保持回测窗口稳定（避免因每日数据更新导致窗口前移）
    result = run_sensitivity_analysis(days=days, periods=periods, window_shift=1)
    
    # 自动生成完整报告
    if auto_report and record_trades:
        print("\n" + "=" * 60)
        print("📊 自动生成回测报告...")
        print("=" * 60)
        
        try:
            # 获取最新的回测会话
            sessions = get_backtest_sessions(limit=1)
            if not sessions.empty:
                session_id = sessions.iloc[0]['session_id']
                _generate_full_report(session_id, save_dir)
                
        except Exception as e:
            print(f"\n⚠️ 报告生成过程中出错: {e}")
            import traceback
            traceback.print_exc()
    
    return result


def _generate_full_report(session_id: str, save_dir: str):
    """生成完整报告（内部函数）
    
    Args:
        session_id: 回测会话 ID
        save_dir: 保存目录
    """
    os.makedirs(save_dir, exist_ok=True)
    
    # 1. 生成可视化报告
    print("\n[1/3] 生成可视化图表...")
    try:
        from momentum.backtest import BacktestVisualizer
        viz = BacktestVisualizer(session_id)
        # 批量生成所有文件但不弹窗
        viz.plot_all(save_dir=save_dir, show=False)
        # 仅自动弹出综合仪表板
        viz.plot_summary_dashboard(show=True)
    except ImportError as e:
        print(f"  ⚠️ 跳过可视化 (缺少 plotly): {e}")
    except Exception as e:
        print(f"  ⚠️ 可视化失败: {e}")
    
    # 2. 生成交易原因分析报告
    print("\n[2/3] 生成交易原因分析...")
    try:
        from momentum.data.trade_reason import analyze_trades_from_db
        report = analyze_trades_from_db(session_id)
        analysis_file = os.path.join(save_dir, f"trade_analysis_{session_id[:8]}.txt")
        with open(analysis_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"  ✅ 已保存: {analysis_file}")
    except Exception as e:
        print(f"  ⚠️ 交易分析失败: {e}")
    
    # 3. 显示会话统计摘要
    print("\n[3/3] 回测统计摘要...")
    _print_session_summary(session_id)
    
    # 打印生成结果
    print("\n" + "=" * 60)
    print(f"✅ 所有报告已生成完成!")
    print(f"📁 报告目录: {os.path.abspath(save_dir)}")
    print("=" * 60)
    print("\n生成的文件:")
    print(f"  • backtest_{session_id[:8]}_equity.html      - 净值曲线")
    print(f"  • backtest_{session_id[:8]}_trades.html      - 交易分析")
    print(f"  • backtest_{session_id[:8]}_timeline.html    - 交易时间线")
    print(f"  • backtest_{session_id[:8]}_dashboard.html   - 综合仪表板")
    print(f"  • backtest_{session_id[:8]}_trade_log.html   - 交易操作日志")
    print(f"  • backtest_{session_id[:8]}_操作指南.txt      - 可操作交易清单")
    print(f"  • trade_analysis_{session_id[:8]}.txt        - 详细交易原因分析")
    print()


def _print_session_summary(session_id: str):
    """打印会话统计摘要（内部函数）"""
    from momentum.backtest import get_backtest_sessions, get_trade_statistics
    
    sessions = get_backtest_sessions(limit=100)
    session_info = sessions[sessions['session_id'] == session_id]
    
    if session_info.empty:
        return
    
    row = session_info.iloc[0]
    stats = get_trade_statistics(session_id)
    
    print("\n" + "█" * 60)
    print(f"📈 回测统计摘要")
    print("-" * 60)
    print(f"  会话ID:     {session_id[:12]}")
    print(f"  回测天数:   {row.get('backtest_days', 0)} 天")
    print(f"  持仓周期:   {row.get('hold_period', 0)} 天")
    print("-" * 60)
    print(f"  总收益:     {row.get('total_return', 0) or 0:+.2f}%")
    print(f"  年化收益:   {row.get('annual_return', 0) or 0:+.2f}%")
    print(f"  夏普比率:   {row.get('sharpe_ratio', 0) or 0:.2f}")
    print(f"  最大回撤:   {row.get('max_drawdown', 0) or 0:.2f}%")
    print("-" * 60)
    
    if stats:
        print(f"  总交易次数: {stats.get('total_trades', 0)}")
        print(f"  胜率:       {stats.get('win_rate', 0):.1f}%")
        print(f"  平均收益:   {stats.get('avg_pnl_pct', 0):+.2f}%")
        
        exit_stats = stats.get('exit_stats', {})
        if exit_stats:
            print("-" * 60)
            print("  退出原因分布:")
            for reason, count in exit_stats.items():
                if reason == 'ATR_Stop':
                    desc = "ATR止损"
                elif reason == 'MA5_Exit':
                    desc = "MA5止盈"
                else:
                    desc = "到期离场"
                print(f"    • {desc}: {count}笔")
    
    print("█" * 60)


def show_backtest_history(limit: int = 10):
    """显示历史回测记录
    
    Args:
        limit: 显示数量
    """
    from momentum.backtest import get_backtest_sessions
    
    logger.info("=" * 60)
    logger.info("Momentum v16 - 回测历史记录")
    logger.info("=" * 60)
    
    sessions = get_backtest_sessions(limit=limit)
    
    if sessions.empty:
        print("\n暂无回测记录\n")
        return
    
    print("\n" + "█" * 100)
    print("📊 回测历史记录")
    print("-" * 100)
    print(f"{'会话ID':<14} {'开始时间':<20} {'天数':<6} {'周期':<6} {'收益%':<10} {'年化%':<10} {'夏普':<8} {'回撤%':<8} {'胜率%':<8} {'交易数':<8}")
    print("-" * 100)
    
    for _, row in sessions.iterrows():
        session_id = row.get('session_id', '')[:12]
        start_time = str(row.get('start_time', ''))[:16]
        days = row.get('backtest_days', 0)
        hold = row.get('hold_period', 0)
        total_ret = row.get('total_return', 0) or 0
        annual_ret = row.get('annual_return', 0) or 0
        sharpe = row.get('sharpe_ratio', 0) or 0
        max_dd = row.get('max_drawdown', 0) or 0
        win_rate = row.get('win_rate', 0) or 0
        trades = row.get('total_trades', 0) or 0
        
        print(f"{session_id:<14} {start_time:<20} {days:<6} {hold:<6} {total_ret:<10.2f} {annual_ret:<10.2f} {sharpe:<8.2f} {max_dd:<8.2f} {win_rate:<8.1f} {trades:<8}")
    
    print("█" * 100)
    print("\n提示: 使用 --session <session_id> 查看详细交易记录\n")


def show_session_detail(session_id: str):
    """显示指定回测会话的详细交易记录
    
    Args:
        session_id: 会话 ID
    """
    from momentum.backtest import get_session_trades, get_trade_statistics
    
    logger.info("=" * 60)
    logger.info(f"Momentum v16 - 回测详情: {session_id}")
    logger.info("=" * 60)
    
    # 获取交易统计
    stats = get_trade_statistics(session_id)
    if not stats:
        print(f"\n未找到会话: {session_id}\n")
        return
    
    print("\n" + "█" * 80)
    print(f"📈 交易统计 (会话: {session_id})")
    print("-" * 80)
    print(f"总交易次数: {stats.get('total_trades', 0)}")
    print(f"盈利交易: {stats.get('win_trades', 0)} | 亏损交易: {stats.get('loss_trades', 0)}")
    print(f"胜率: {stats.get('win_rate', 0):.1f}%")
    print(f"平均收益: {stats.get('avg_pnl_pct', 0):.2f}%")
    print(f"最大盈利: {stats.get('max_win_pct', 0):.2f}% | 最大亏损: {stats.get('max_loss_pct', 0):.2f}%")
    print(f"平均持仓天数: {stats.get('avg_hold_days', 0):.1f}天")
    print("-" * 80)
    
    # 退出原因统计
    exit_stats = stats.get('exit_stats', {})
    if exit_stats:
        print("退出原因分布:")
        for reason, count in exit_stats.items():
            print(f"  - {reason}: {count}笔")
    
    print("█" * 80)
    
    # 获取交易明细
    trades = get_session_trades(session_id)
    if not trades.empty:
        sells = trades[trades['trade_type'] == 'SELL'].copy()
        if not sells.empty:
            print("\n📋 最近交易明细 (最多显示20条):")
            print("-" * 100)
            print(f"{'日期':<12} {'代码':<8} {'名称':<10} {'买入价':<10} {'卖出价':<10} {'收益%':<10} {'持仓天数':<8} {'退出原因':<12}")
            print("-" * 100)
            
            for _, sell in sells.tail(20).iterrows():
                code = sell['code']
                buys = trades[(trades['code'] == code) & (trades['trade_type'] == 'BUY')]
                buy_price = buys['price'].iloc[-1] if not buys.empty else 0
                
                print(f"{sell['trade_date']:<12} {sell['code']:<8} {str(sell['name'])[:8]:<10} "
                      f"{buy_price:<10.2f} {sell['price']:<10.2f} {sell['pnl_pct'] or 0:<10.2f} "
                      f"{sell['hold_days'] or 0:<8} {sell['exit_reason'] or '':<12}")
    
    print()


def run_visualize(session_id: Optional[str] = None, save_dir: Optional[str] = None):
    """可视化回测结果
    
    Args:
        session_id: 会话 ID（可选，默认使用最近一次）
        save_dir: 保存目录（可选）
    """
    from momentum.backtest import visualize_latest_backtest, visualize_session
    
    logger.info("=" * 60)
    logger.info("Momentum v16 - 回测可视化")
    logger.info("=" * 60)
    
    try:
        if session_id:
            visualize_session(session_id, show=True, save_dir=save_dir)
        else:
            visualize_latest_backtest(show=True, save_dir=save_dir)
    except ImportError as e:
        print(f"\n❌ 缺少依赖: {e}")
        print("请安装 plotly: pip install plotly")
    except ValueError as e:
        print(f"\n❌ 错误: {e}")
