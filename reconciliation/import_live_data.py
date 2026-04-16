# -*- coding: utf-8 -*-
"""
实盘交易数据导入工具

支持多种导入方式：
1. CSV 文件导入
2. 命令行手动录入
3. 自动从券商API导入（待实现）
"""

import pandas as pd
import sqlite3
import os
import sys
from datetime import datetime
from typing import List, Dict

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

DB_PATH = os.path.join(parent_dir, 'qlib_pro_v16.db')


def _get_exchange(code: str) -> str:
    """根据股票代码获取交易所前缀"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return 'sh'  # 上交所
    elif code.startswith(('0', '3')):
        return 'sz'  # 深交所
    elif code.startswith('4') or code.startswith('8'):
        return 'bj'  # 北交所
    else:
        return 'sz'  # 默认深交所


def import_trades_from_csv(csv_file: str, clear_existing: bool = False):
    """
    从CSV导入交易记录
    
    CSV格式要求:
    code,name,trade_type,trade_date,trade_time,price,shares,commission,notes
    000001,平安银行,BUY,2025-02-05,09:35:00,11.50,1000,5.75,
    000002,万科A,SELL,2025-02-05,14:30:00,8.80,500,2.20,止损退出
    """
    if not os.path.exists(csv_file):
        print(f"❌ 文件不存在: {csv_file}")
        return False
    
    try:
        # 读取CSV，code字段必须是字符串类型
        df = pd.read_csv(csv_file, dtype={'code': str})
        
        # 补齐股票代码为6位
        df['code'] = df['code'].str.zfill(6)
        
        # 验证必需字段
        required_cols = ['code', 'trade_type', 'trade_date', 'price', 'shares']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"❌ CSV缺少必需字段: {missing_cols}")
            return False
        
        # 处理可选字段
        if 'name' not in df.columns:
            df['name'] = None
        if 'trade_time' not in df.columns:
            df['trade_time'] = None
        if 'commission' not in df.columns:
            df['commission'] = df['shares'] * df['price'] * 0.0003  # 默认万分之三
        if 'notes' not in df.columns:
            df['notes'] = None
        if 'status' not in df.columns:
            df['status'] = 'success'
        
        # 计算成交金额
        df['amount'] = df['shares'] * df['price']
        
        conn = sqlite3.connect(DB_PATH)
        
        # 如果需要清空现有数据
        if clear_existing:
            conn.execute("DELETE FROM live_trades")
            print(f"⚠️ 已清空现有交易记录")
        
        # 插入数据
        df.to_sql('live_trades', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        
        print(f"✅ 成功导入 {len(df)} 条交易记录")
        print(f"   买入: {len(df[df['trade_type']=='BUY'])} 笔")
        print(f"   卖出: {len(df[df['trade_type']=='SELL'])} 笔")
        
        # 自动计算持仓和收益
        calc_positions_from_trades()
        calc_pnl_from_positions()
        
        return True
        
    except Exception as e:
        print(f"❌ 导入失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def import_positions_from_csv(csv_file: str):
    """
    从CSV导入持仓数据
    
    CSV格式:
    code,name,shares,cost_price,current_price,update_date,update_time
    000001,平安银行,1000,11.50,11.60,2025-02-05,15:00:00
    600036,招商银行,800,39.20,39.50,2025-02-05,15:00:00
    """
    if not os.path.exists(csv_file):
        print(f"❌ 文件不存在: {csv_file}")
        return False
    
    try:
        # 读取CSV，code字段必须是字符串类型
        df = pd.read_csv(csv_file, dtype={'code': str})
        
        # 补全代码格式为6位
        df['code'] = df['code'].str.zfill(6)
        
        # 验证必需字段
        required_cols = ['code', 'shares', 'cost_price', 'update_date']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"❌ CSV缺少必需字段: {missing_cols}")
            return False
        
        # 处理可选字段
        if 'name' not in df.columns:
            df['name'] = None

        # 自动补齐更新时间
        now = datetime.now()
        if 'update_date' not in df.columns:
            df['update_date'] = now.strftime('%Y-%m-%d')
        else:
            df['update_date'] = df['update_date'].fillna(now.strftime('%Y-%m-%d'))

        if 'update_time' not in df.columns:
            df['update_time'] = now.strftime('%H:%M:%S')
        else:
            df['update_time'] = df['update_time'].fillna(now.strftime('%H:%M:%S'))

        # 自动拉取实时价格
        if 'current_price' not in df.columns:
            df['current_price'] = None

        df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce')
        need_price_mask = df['current_price'].isna() | (df['current_price'] <= 0)
        if need_price_mask.any():
            price_map = _fetch_realtime_prices(df.loc[need_price_mask, 'code'].tolist())
            if price_map:
                df.loc[need_price_mask, 'current_price'] = df.loc[need_price_mask, 'code'].map(price_map)

        # 仍然缺失时回退为成本价
        df['current_price'] = df['current_price'].fillna(df['cost_price'])
        
        # 计算市值和盈亏
        df['market_value'] = df['shares'] * df['current_price']
        df['pnl'] = (df['current_price'] - df['cost_price']) * df['shares']
        df['pnl_pct'] = (df['current_price'] / df['cost_price'] - 1) * 100
        
        conn = sqlite3.connect(DB_PATH)
        
        # 替换式更新（先删除同一日期的持仓）
        update_date = df['update_date'].iloc[0]
        conn.execute("DELETE FROM live_positions WHERE update_date = ?", (update_date,))
        
        # 插入新持仓
        df.to_sql('live_positions', conn, if_exists='append', index=False, 
                 dtype={'code': 'TEXT'})
        
        conn.commit()
        conn.close()
        
        print(f"✅ 成功导入 {len(df)} 个持仓")
        print(f"   持仓总市值: {df['market_value'].sum():.2f}")
        print(f"   浮动盈亏: {df['pnl'].sum():+.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ 导入失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def _fetch_realtime_prices(codes: List[str]) -> Dict[str, float]:
    """获取实时价格（新浪接口）"""
    if not codes:
        return {}

    try:
        import requests
        
        # 构建查询列表
        query_list = []
        for code in codes:
            code = str(code).zfill(6)
            exch = _get_exchange(code)
            query_list.append(f"{exch}{code}")
        
        # 请求新浪接口
        url = f"https://hq.sinajs.cn/list={','.join(query_list)}"
        headers = {'Referer': 'https://finance.sina.com.cn/'}
        
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code != 200:
            return {}
        
        # 解析数据
        price_map = {}
        lines = resp.text.strip().split('\n')
        
        for line in lines:
            if not line or '=' not in line:
                continue
            
            try:
                # var hq_str_sz000001="平安银行,10.50,..."
                eq_idx = line.find('=')
                code_part = line[:eq_idx]
                data_part = line[eq_idx+1:].strip('"')
                
                # 提取代码 (去掉前缀)
                code_with_exch = code_part.split('_')[-1]
                code = code_with_exch[2:]  # 去掉 sh/sz 前缀
                
                # 解析数据字段
                fields = data_part.split(',')
                if len(fields) < 4:
                    continue
                
                # 第3个字段是最新价 (索引3)
                current_price = float(fields[3])
                
                if current_price > 0:
                    price_map[code] = current_price
                    
            except Exception:
                continue
        
        return price_map
        
    except Exception:
        return {}


def calc_positions_from_trades(silent: bool = False) -> bool:
    """
    从交易记录自动计算持仓
    
    逻辑：
    1. 读取所有交易记录
    2. 按股票代码分组
    3. 计算净持仓 = 买入总股数 - 卖出总股数
    4. 计算加权平均成本价
    5. 拉取实时价格
    6. 更新 live_positions 表
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # 读取所有交易记录
        trades_df = pd.read_sql_query("""
            SELECT code, name, trade_type, price, shares
            FROM live_trades
            ORDER BY trade_date, trade_time
        """, conn)
        
        if trades_df.empty:
            if not silent:
                print("⚠️ 无交易记录，跳过持仓计算")
            conn.close()
            return True
        
        # 按股票代码分组计算
        positions = []
        
        for code, group in trades_df.groupby('code'):
            name = group['name'].iloc[0] if pd.notna(group['name'].iloc[0]) else None
            
            # 分离买入和卖出
            buys = group[group['trade_type'] == 'BUY']
            sells = group[group['trade_type'] == 'SELL']
            
            # 计算净持仓
            buy_shares = buys['shares'].sum() if not buys.empty else 0
            sell_shares = sells['shares'].sum() if not sells.empty else 0
            net_shares = buy_shares - sell_shares
            
            # 如果已清仓，跳过
            if net_shares <= 0:
                continue
            
            # 计算加权平均成本价（简化版：总买入成本 / 总买入股数）
            # 更精确的算法需要考虑卖出的先进先出
            if not buys.empty:
                total_cost = (buys['price'] * buys['shares']).sum()
                cost_price = total_cost / buy_shares
            else:
                cost_price = 0
            
            positions.append({
                'code': code,
                'name': name,
                'shares': int(net_shares),
                'cost_price': round(cost_price, 2)
            })
        
        if not positions:
            if not silent:
                print("⚠️ 无持仓数据（可能已全部清仓）")
            conn.close()
            return True
        
        # 转为 DataFrame
        pos_df = pd.DataFrame(positions)
        
        # 确保 code 是6位字符串格式
        pos_df['code'] = pos_df['code'].astype(str).str.zfill(6)
        
        # 拉取实时价格
        price_map = _fetch_realtime_prices(pos_df['code'].tolist())
        pos_df['current_price'] = pos_df['code'].map(price_map)
        pos_df['current_price'] = pos_df['current_price'].fillna(pos_df['cost_price'])
        
        # 计算市值和盈亏
        pos_df['market_value'] = pos_df['shares'] * pos_df['current_price']
        pos_df['pnl'] = (pos_df['current_price'] - pos_df['cost_price']) * pos_df['shares']
        pos_df['pnl_pct'] = (pos_df['current_price'] / pos_df['cost_price'] - 1) * 100
        
        # 添加更新时间
        now = datetime.now()
        pos_df['update_date'] = now.strftime('%Y-%m-%d')
        pos_df['update_time'] = now.strftime('%H:%M:%S')
        
        # 清空并插入新持仓
        conn.execute("DELETE FROM live_positions")
        pos_df.to_sql('live_positions', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        
        if not silent:
            print(f"\n🔄 自动计算持仓完成")
            print(f"   持仓品种: {len(pos_df)} 个")
            print(f"   持仓总市值: {pos_df['market_value'].sum():.2f}")
            print(f"   浮动盈亏: {pos_df['pnl'].sum():+.2f} ({pos_df['pnl'].sum() / (pos_df['shares'] * pos_df['cost_price']).sum() * 100:+.2f}%)")
        
        return True
        
    except Exception as e:
        if not silent:
            print(f"❌ 持仓计算失败: {str(e)}")
            import traceback
            traceback.print_exc()
        return False


def calc_pnl_from_positions(silent: bool = False) -> bool:
    """
    从持仓自动计算每日收益
    
    逻辑：
    1. 读取当前持仓
    2. 计算总市值
    3. 对比前一日总市值计算日收益
    4. 更新 live_daily_pnl 表
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # 读取当前持仓
        pos_df = pd.read_sql_query("""
            SELECT code, shares, cost_price, current_price, market_value, pnl, update_date
            FROM live_positions
        """, conn)
        
        if pos_df.empty:
            if not silent:
                print("⚠️ 无持仓数据，跳过收益计算")
            conn.close()
            return True
        
        # 计算总市值和总盈亏
        total_value = pos_df['market_value'].sum()
        total_cost = (pos_df['shares'] * pos_df['cost_price']).sum()
        total_pnl = pos_df['pnl'].sum()
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        
        # 获取交易日期（使用持仓更新日期）
        trade_date = pos_df['update_date'].iloc[0]
        
        # 查询前一日总市值
        prev_pnl = pd.read_sql_query("""
            SELECT total_value
            FROM live_daily_pnl
            WHERE trade_date < ?
            ORDER BY trade_date DESC
            LIMIT 1
        """, conn, params=(trade_date,))
        
        if not prev_pnl.empty:
            prev_value = prev_pnl['total_value'].iloc[0]
            daily_pnl = total_value - prev_value
            daily_pnl_pct = (daily_pnl / prev_value * 100) if prev_value > 0 else 0
        else:
            # 首日没有对比基准，日收益 = 总收益
            daily_pnl = total_pnl
            daily_pnl_pct = total_pnl_pct
        
        # 构造收益记录
        pnl_record = pd.DataFrame([{
            'trade_date': trade_date,
            'total_value': round(total_value, 2),
            'daily_pnl': round(daily_pnl, 2),
            'daily_pnl_pct': round(daily_pnl_pct, 2),
            'total_pnl': round(total_pnl, 2),
            'total_pnl_pct': round(total_pnl_pct, 2)
        }])
        
        # 删除当日已有记录，插入新记录
        conn.execute("DELETE FROM live_daily_pnl WHERE trade_date = ?", (trade_date,))
        pnl_record.to_sql('live_daily_pnl', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        
        if not silent:
            print(f"\n📈 自动计算收益完成")
            print(f"   日期: {trade_date}")
            print(f"   总市值: {total_value:.2f}")
            print(f"   日收益: {daily_pnl:+.2f} ({daily_pnl_pct:+.2f}%)")
            print(f"   累计收益: {total_pnl:+.2f} ({total_pnl_pct:+.2f}%)")
        
        return True
        
    except Exception as e:
        if not silent:
            print(f"❌ 收益计算失败: {str(e)}")
            import traceback
            traceback.print_exc()
        return False


def import_daily_pnl_from_csv(csv_file: str):
    """
    从CSV导入每日收益数据
    
    CSV格式:
    trade_date,total_value,daily_pnl,daily_pnl_pct,total_pnl,total_pnl_pct
    2025-02-05,102500.00,1200.00,1.18,2500.00,2.50
    """
    if not os.path.exists(csv_file):
        print(f"❌ 文件不存在: {csv_file}")
        return False
    
    try:
        # 读取CSV
        df = pd.read_csv(csv_file)
        
        required_cols = ['trade_date', 'daily_pnl_pct']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"❌ CSV缺少必需字段: {missing_cols}")
            return False
        
        conn = sqlite3.connect(DB_PATH)
        df.to_sql('live_daily_pnl', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
        
        print(f"✅ 成功导入 {len(df)} 条每日收益记录")
        
        return True
        
    except Exception as e:
        print(f"❌ 导入失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def interactive_trade_entry():
    """交互式录入交易"""
    print("\n📝 交互式交易录入")
    print("="*60)
    
    trades = []
    
    while True:
        print("\n输入交易信息（输入q退出）:")
        
        code = input("  股票代码: ").strip()
        if code.lower() == 'q':
            break
        
        name = input("  股票名称: ").strip() or None
        trade_type = input("  交易类型 (BUY/SELL): ").strip().upper()
        trade_date = input("  交易日期 (YYYY-MM-DD): ").strip()
        trade_time = input("  交易时间 (HH:MM:SS, 可选): ").strip() or None
        price = float(input("  成交价格: ").strip())
        shares = int(input("  成交数量: ").strip())
        commission = input("  手续费 (可选，回车自动计算): ").strip()
        commission = float(commission) if commission else shares * price * 0.0003
        notes = input("  备注 (可选): ").strip() or None
        
        amount = shares * price
        
        trades.append({
            'code': code,
            'name': name,
            'trade_type': trade_type,
            'trade_date': trade_date,
            'trade_time': trade_time,
            'price': price,
            'shares': shares,
            'amount': amount,
            'commission': commission,
            'status': 'success',
            'notes': notes
        })
        
        print(f"✓ 已添加: {code} {trade_type} {shares}股 @{price}")
    
    if not trades:
        print("⚠️ 未录入任何交易")
        return False
    
    # 保存到数据库
    df = pd.DataFrame(trades)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('live_trades', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    
    print(f"\n✅ 已保存 {len(trades)} 笔交易到数据库")
    
    # 自动计算持仓和收益
    calc_positions_from_trades()
    calc_pnl_from_positions()
    
    return True


def generate_sample_csv():
    """生成示例CSV文件"""
    
    # 交易记录示例
    trades_sample = pd.DataFrame([
        {
            'code': '000001',
            'name': '平安银行',
            'trade_type': 'BUY',
            'trade_date': '2025-02-05',
            'trade_time': '09:35:00',
            'price': 11.50,
            'shares': 1000,
            'commission': 5.75,
            'notes': ''
        },
        {
            'code': '600036',
            'name': '招商银行',
            'trade_type': 'BUY',
            'trade_date': '2025-02-05',
            'trade_time': '10:00:00',
            'price': 39.20,
            'shares': 500,
            'commission': 9.80,
            'notes': ''
        }
    ])
    
    # 持仓示例（最小化字段，其余自动填充）
    positions_sample = pd.DataFrame([
        {
            'code': '000001',
            'name': '平安银行',
            'shares': 1000,
            'cost_price': 11.50
        },
        {
            'code': '600036',
            'name': '招商银行',
            'shares': 500,
            'cost_price': 39.20
        }
    ])
    
    # 每日收益示例
    pnl_sample = pd.DataFrame([
        {
            'trade_date': '2025-02-05',
            'total_value': 31050.00,
            'daily_pnl': 250.00,
            'daily_pnl_pct': 0.81,
            'total_pnl': 250.00,
            'total_pnl_pct': 0.81
        }
    ])
    
    trades_sample.to_csv('sample_trades.csv', index=False, encoding='utf-8-sig')
    positions_sample.to_csv('sample_positions.csv', index=False, encoding='utf-8-sig')
    pnl_sample.to_csv('sample_daily_pnl.csv', index=False, encoding='utf-8-sig')
    
    print("✅ 已生成示例CSV文件:")
    print("   - sample_trades.csv (交易记录)")
    print("   - sample_positions.csv (持仓 - 可选，会自动计算)")
    print("   - sample_daily_pnl.csv (每日收益 - 可选，会自动计算)")
    print("")
    print("💡 提示: 导入交易记录后会自动计算持仓和收益")
    print("   手动触发: --calc-positions 或 --calc-pnl")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='实盘交易数据导入工具')
    parser.add_argument('--trades', type=str, help='导入交易记录CSV')
    parser.add_argument('--positions', type=str, help='导入持仓CSV')
    parser.add_argument('--pnl', type=str, help='导入每日收益CSV')
    parser.add_argument('--clear', action='store_true', help='清空现有交易记录')
    parser.add_argument('--interactive', action='store_true', help='交互式录入')
    parser.add_argument('--sample', action='store_true', help='生成示例CSV文件')
    parser.add_argument('--calc-positions', action='store_true', help='从交易记录计算持仓')
    parser.add_argument('--calc-pnl', action='store_true', help='从持仓计算每日收益')
    
    args = parser.parse_args()
    
    print("\n📊 实盘交易数据导入工具")
    print("="*60)
    
    if args.sample:
        generate_sample_csv()
        return 0
    
    if args.interactive:
        interactive_trade_entry()
        return 0
    
    success = True
    
    if args.trades:
        print(f"\n导入交易记录: {args.trades}")
        success &= import_trades_from_csv(args.trades, args.clear)
    
    if args.positions:
        print(f"\n导入持仓数据: {args.positions}")
        success &= import_positions_from_csv(args.positions)
    
    if args.pnl:
        print(f"\n导入每日收益: {args.pnl}")
        success &= import_daily_pnl_from_csv(args.pnl)
    
    if args.calc_positions:
        print("\n🔄 从交易记录计算持仓...")
        success &= calc_positions_from_trades()
    
    if args.calc_pnl:
        print("\n📈 从持仓计算每日收益...")
        success &= calc_pnl_from_positions()
    
    if not (args.trades or args.positions or args.pnl or args.calc_positions or args.calc_pnl):
        parser.print_help()
        print("\n提示: 使用 --sample 生成示例CSV文件")
        print("      使用 --interactive 交互式录入")
        print("      导入交易记录后会自动计算持仓和收益")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
