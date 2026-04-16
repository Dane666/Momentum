#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""分析回测持仓数据"""

import sqlite3
import pandas as pd

# 直接连接数据库
conn = sqlite3.connect('qlib_pro_v16.db')

# 获取最新会话
sessions = pd.read_sql('SELECT * FROM backtest_sessions ORDER BY start_time DESC LIMIT 1', conn)
if sessions.empty:
    print('无回测数据')
else:
    session_id = sessions.iloc[0]['session_id']
    print(f'会话ID: {session_id[:12]}')
    print(f'回测天数: {sessions.iloc[0]["backtest_days"]}')
    print(f'持仓周期: {sessions.iloc[0]["hold_period"]}')
    
    # 获取净值曲线 (查看每期持仓数量)
    equity = pd.read_sql(
        f"SELECT trade_date, position_count, nav FROM backtest_equity_curve WHERE session_id='{session_id}' ORDER BY trade_date", 
        conn
    )
    if not equity.empty:
        print(f'\n=== 每期持仓数量 (position_count) ===')
        print(equity.to_string())
    else:
        print('净值曲线表为空')
    
    # 统计交易
    trades = pd.read_sql(
        f"SELECT buy_date, code, name, exit_reason FROM backtest_trades WHERE session_id='{session_id}' ORDER BY buy_date", 
        conn
    )
    if not trades.empty:
        print(f'\n=== 交易统计 ===')
        print(f'总交易笔数: {len(trades)}')
        buy_dates = trades['buy_date'].unique()
        print(f'换仓次数: {len(buy_dates)}')
        for d in sorted(buy_dates):
            subset = trades[trades['buy_date'] == d]
            count = len(subset)
            codes = subset['code'].tolist()
            print(f'  {d}: {count}只 - {codes}')
        
        # 退出原因统计
        print(f'\n=== 退出原因分布 ===')
        exit_stats = trades['exit_reason'].value_counts()
        for reason, count in exit_stats.items():
            print(f'  {reason}: {count}笔')

conn.close()
