#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库查询工具 - 快速查看回测和实盘数据

用法:
    python3 query_db.py --help
    python3 query_db.py --backtest-sessions
    python3 query_db.py --backtest-trades --date 2025-02-05
    python3 query_db.py --live-trades --date 2025-02-05
    python3 query_db.py --live-positions
"""

import sqlite3
import pandas as pd
import os
import sys
import argparse
from datetime import datetime, timedelta

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

DB_PATH = os.path.join(parent_dir, 'qlib_pro_v16.db')


def format_table(df, max_rows=None):
    """格式化输出表格"""
    if df.empty:
        print("  (无数据)")
        return
    
    if max_rows and len(df) > max_rows:
        df = df.head(max_rows)
        print(df.to_string(index=False))
        print(f"  ... (共 {len(df)} 行，已显示前 {max_rows} 行)")
    else:
        print(df.to_string(index=False))


def query_backtest_sessions():
    """查询回测会话"""
    print("\n📊 回测会话列表")
    print("=" * 80)
    
    query = '''
        SELECT 
            session_id,
            start_time,
            end_time,
            status,
            COUNT(DISTINCT code) as trade_count
        FROM backtest_sessions
        LEFT JOIN backtest_trades USING (session_id)
        GROUP BY session_id
        ORDER BY start_time DESC
    '''
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    format_table(df)
    print()


def query_backtest_trades(session_id=None, date_range=None, limit=20):
    """查询回测交易"""
    print("\n💹 回测交易记录")
    print("=" * 80)
    
    conn = sqlite3.connect(DB_PATH)
    
    if session_id is None:
        # 获取最新会话
        latest_query = "SELECT session_id FROM backtest_sessions WHERE status='completed' ORDER BY start_time DESC LIMIT 1"
        latest = pd.read_sql_query(latest_query, conn)
        if latest.empty:
            print("  (无回测数据)")
            conn.close()
            return
        session_id = latest['session_id'].iloc[0]
        print(f"  使用最新会话: {session_id}\n")
    
    query = f'''
        SELECT 
            code,
            name,
            trade_type,
            trade_date,
            price,
            shares,
            pnl,
            COALESCE(exit_reason, '') as exit_reason
        FROM backtest_trades
        WHERE session_id = '{session_id}'
    '''
    
    if date_range:
        start_date, end_date = date_range
        query += f" AND trade_date BETWEEN '{start_date}' AND '{end_date}'"
    
    query += f" ORDER BY trade_date DESC, code LIMIT {limit}"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        # 格式化数值列
        df['price'] = df['price'].apply(lambda x: f"{x:.2f}")
        df['pnl'] = df['pnl'].apply(lambda x: f"{x:+.2f}")
    
    format_table(df)
    print()


def query_backtest_positions(session_id=None, date=None):
    """查询回测持仓"""
    print("\n📍 回测持仓")
    print("=" * 80)
    
    conn = sqlite3.connect(DB_PATH)
    
    if session_id is None:
        # 获取最新会话
        latest_query = "SELECT session_id FROM backtest_sessions WHERE status='completed' ORDER BY start_time DESC LIMIT 1"
        latest = pd.read_sql_query(latest_query, conn)
        if latest.empty:
            print("  (无回测数据)")
            conn.close()
            return
        session_id = latest['session_id'].iloc[0]
    
    if date is None:
        # 获取最新日期
        date_query = f"SELECT DISTINCT trade_date FROM backtest_positions WHERE session_id='{session_id}' ORDER BY trade_date DESC LIMIT 1"
        date_result = pd.read_sql_query(date_query, conn)
        if date_result.empty:
            print("  (无持仓数据)")
            conn.close()
            return
        date = date_result['trade_date'].iloc[0]
    
    print(f"  会话: {session_id}")
    print(f"  日期: {date}\n")
    
    query = f'''
        SELECT 
            code,
            name,
            shares,
            cost_price,
            current_price,
            market_value,
            unrealized_pnl,
            unrealized_pnl_pct
        FROM backtest_positions
        WHERE session_id = '{session_id}'
        AND trade_date = '{date}'
        ORDER BY code
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['cost_price'] = df['cost_price'].apply(lambda x: f"{x:.2f}")
        df['current_price'] = df['current_price'].apply(lambda x: f"{x:.2f}")
        df['market_value'] = df['market_value'].apply(lambda x: f"{x:,.2f}")
        df['unrealized_pnl'] = df['unrealized_pnl'].apply(lambda x: f"{x:+,.2f}")
        df['unrealized_pnl_pct'] = df['unrealized_pnl_pct'].apply(lambda x: f"{x:+.2f}%")
    
    format_table(df)
    print()


def query_live_trades(date=None, limit=20):
    """查询实盘交易"""
    print("\n📈 实盘交易记录")
    print("=" * 80)
    
    query = 'SELECT code, name, trade_type, trade_date, trade_time, price, shares, commission, status, notes FROM live_trades'
    
    if date:
        query += f" WHERE trade_date = '{date}'"
    
    query += f" ORDER BY trade_date DESC, trade_time DESC LIMIT {limit}"
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['price'] = df['price'].apply(lambda x: f"{x:.2f}")
        df['commission'] = df['commission'].apply(lambda x: f"{x:.2f}")
        format_table(df)
    else:
        print("  (无实盘交易记录)")
    
    print()


def query_live_positions(date=None):
    """查询实盘持仓"""
    print("\n💰 实盘持仓")
    print("=" * 80)
    
    query = '''
        SELECT 
            code,
            name,
            shares,
            cost_price,
            current_price,
            market_value,
            pnl,
            pnl_pct
        FROM live_positions
    '''
    
    if date:
        query += f" WHERE update_date = '{date}'"
    
    query += " ORDER BY code"
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['cost_price'] = df['cost_price'].apply(lambda x: f"{x:.2f}")
        df['current_price'] = df['current_price'].apply(lambda x: f"{x:.2f}")
        df['market_value'] = df['market_value'].apply(lambda x: f"{x:,.2f}")
        df['pnl'] = df['pnl'].apply(lambda x: f"{x:+,.2f}")
        df['pnl_pct'] = df['pnl_pct'].apply(lambda x: f"{x:+.2f}%")
        
        # 计算总计
        print()
        format_table(df)
        
        conn = sqlite3.connect(DB_PATH)
        total = pd.read_sql_query(
            'SELECT SUM(market_value) as total_value, SUM(pnl) as total_pnl FROM live_positions', 
            conn
        )
        conn.close()
        
        if not total.empty and total['total_value'].iloc[0] is not None:
            total_value = total['total_value'].iloc[0]
            total_pnl = total['total_pnl'].iloc[0]
            pnl_pct = (total_pnl / (total_value - total_pnl)) * 100
            print(f"\n  汇总:")
            print(f"    总市值: ¥{total_value:,.2f}")
            print(f"    浮动盈亏: ¥{total_pnl:+,.2f} ({pnl_pct:+.2f}%)")
    else:
        print("  (无实盘持仓)")
    
    print()


def query_live_daily_pnl(date_range=None, limit=10):
    """查询实盘每日收益"""
    print("\n📊 实盘每日收益")
    print("=" * 80)
    
    query = '''
        SELECT 
            trade_date,
            daily_pnl,
            daily_pnl_pct,
            total_pnl,
            total_pnl_pct
        FROM live_daily_pnl
    '''
    
    if date_range:
        start_date, end_date = date_range
        query += f" WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'"
    
    query += f" ORDER BY trade_date DESC LIMIT {limit}"
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['daily_pnl'] = df['daily_pnl'].apply(lambda x: f"{x:+,.2f}")
        df['daily_pnl_pct'] = df['daily_pnl_pct'].apply(lambda x: f"{x:+.2f}%")
        df['total_pnl'] = df['total_pnl'].apply(lambda x: f"{x:+,.2f}")
        df['total_pnl_pct'] = df['total_pnl_pct'].apply(lambda x: f"{x:+.2f}%")
        
        format_table(df)
    else:
        print("  (无每日收益记录)")
    
    print()


def query_equity_curve(session_id=None, limit=10):
    """查询权益曲线"""
    print("\n📈 回测权益曲线")
    print("=" * 80)
    
    conn = sqlite3.connect(DB_PATH)
    
    if session_id is None:
        latest_query = "SELECT session_id FROM backtest_sessions WHERE status='completed' ORDER BY start_time DESC LIMIT 1"
        latest = pd.read_sql_query(latest_query, conn)
        if latest.empty:
            print("  (无回测数据)")
            conn.close()
            return
        session_id = latest['session_id'].iloc[0]
    
    print(f"  会话: {session_id}\n")
    
    query = f'''
        SELECT 
            trade_date,
            total_value,
            daily_return,
            cumulative_return
        FROM backtest_equity_curve
        WHERE session_id = '{session_id}'
        ORDER BY trade_date DESC
        LIMIT {limit}
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['total_value'] = df['total_value'].apply(lambda x: f"{x:,.2f}")
        df['daily_return'] = df['daily_return'].apply(lambda x: f"{x:+.2f}%")
        df['cumulative_return'] = df['cumulative_return'].apply(lambda x: f"{x:+.2f}%")
        
        format_table(df)
    else:
        print("  (无权益数据)")
    
    print()


def main():
    parser = argparse.ArgumentParser(description='数据库查询工具')
    
    parser.add_argument('--backtest-sessions', action='store_true', help='查询回测会话')
    parser.add_argument('--backtest-trades', action='store_true', help='查询回测交易')
    parser.add_argument('--backtest-positions', action='store_true', help='查询回测持仓')
    parser.add_argument('--equity-curve', action='store_true', help='查询权益曲线')
    
    parser.add_argument('--live-trades', action='store_true', help='查询实盘交易')
    parser.add_argument('--live-positions', action='store_true', help='查询实盘持仓')
    parser.add_argument('--live-daily-pnl', action='store_true', help='查询实盘每日收益')
    
    parser.add_argument('--session', type=str, help='回测会话ID（可选）')
    parser.add_argument('--date', type=str, help='日期 YYYY-MM-DD')
    parser.add_argument('--limit', type=int, default=10, help='返回行数限制')
    
    args = parser.parse_args()
    
    print(f"\n{'='*80}")
    print(f"数据库查询工具")
    print(f"{'='*80}")
    
    # 检查数据库
    if not os.path.exists(DB_PATH):
        print(f"❌ 数据库不存在: {DB_PATH}")
        return 1
    
    # 默认查询
    if not any([args.backtest_sessions, args.backtest_trades, args.backtest_positions, 
                args.live_trades, args.live_positions, args.live_daily_pnl, 
                args.equity_curve]):
        # 显示概览
        print("\n📋 数据库概览")
        print("-" * 80)
        query_backtest_sessions()
        query_backtest_positions()
        query_live_positions()
        query_live_daily_pnl()
    
    else:
        if args.backtest_sessions:
            query_backtest_sessions()
        
        if args.backtest_trades:
            query_backtest_trades(args.session, limit=args.limit)
        
        if args.backtest_positions:
            query_backtest_positions(args.session, args.date)
        
        if args.equity_curve:
            query_equity_curve(args.session, args.limit)
        
        if args.live_trades:
            query_live_trades(args.date, args.limit)
        
        if args.live_positions:
            query_live_positions(args.date)
        
        if args.live_daily_pnl:
            query_live_daily_pnl(limit=args.limit)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
