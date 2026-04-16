# -*- coding: utf-8 -*-
"""
网格交易相关命令
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Optional
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from momentum.strategies.grid_trading_screener import GridTradingScreener
from momentum.data import load_or_fetch_kline
from momentum.data.fetcher import fetch_kline_from_api

logger = logging.getLogger('momentum')


def run_grid_screening(
    target_type: str = 'etf',
    lookback_years: int = 3,
    min_score: int = 60,
    top_n: int = 20
):
    """
    执行网格交易标的筛选
    
    Args:
        target_type: 标的类型 ('etf', 'stock', 'all')
        lookback_years: 回看年数
        min_score: 最低评分
        top_n: 返回前N个结果
    """
    print("=" * 80)
    print("网格交易标的筛选器")
    print("=" * 80)
    print(f"\n筛选配置:")
    print(f"  标的类型: {target_type.upper()}")
    print(f"  历史数据: 近 {lookback_years} 年")
    print(f"  最低评分: {min_score} 分")
    print(f"  返回数量: Top {top_n}")
    print("\n筛选标准:")
    print("  1. 年化波动率: 15%-40% (适中波动)")
    print("  2. 趋势强度: R² < 0.5 (横盘震荡)")
    print("  3. 流动性: 日均成交额 > 5000万")
    print("  4. 历年3月份表现分析")
    print("  5. 两会期间(3月1-20日)表现分析")
    print("-" * 80)
    
    # 初始化筛选器
    screener = GridTradingScreener(lookback_years=lookback_years)
    
    # 获取标的列表
    candidates = get_candidates_list(target_type)
    
    print(f"\n开始筛选 {len(candidates)} 个标的...")
    print("(注: 只显示评分 >= {min_score} 的结果)\n")
    
    results = []
    success_count = 0
    fail_count = 0
    
    for i, (code, name) in enumerate(candidates, 1):
        print(f"[{i}/{len(candidates)}] {code} {name}...", end=' ')
        
        try:
            # 获取历史数据
            df = get_historical_data(code, lookback_years)
            
            if df is None or df.empty:
                print("× 无数据")
                fail_count += 1
                continue
            
            # 筛选
            result = screener.screen_single_stock(code, name, df)
            
            if result and result['grid_score'] >= min_score:
                results.append(result)
                print(f"✓ 评分: {result['grid_score']:.0f}")
                success_count += 1
            else:
                print("× 不符合")
                fail_count += 1
                
        except Exception as e:
            print(f"× 错误: {str(e)[:30]}")
            fail_count += 1
            continue
    
    # 输出结果
    print("\n" + "=" * 80)
    print("筛选结果汇总")
    print("=" * 80)
    print(f"总计: {len(candidates)} | 符合: {success_count} | 不符合: {fail_count}")
    
    if results:
        # 格式化结果
        df_result = screener.format_screening_result(results)
        
        # 只显示top_n
        if len(df_result) > top_n:
            df_display = df_result.head(top_n)
            print(f"\n显示Top {top_n}个最佳标的:")
        else:
            df_display = df_result
            print(f"\n共 {len(df_result)} 个符合条件的标的:")
        
        print(df_display.to_string(index=False))
        
        # 保存完整结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'grid_trading_candidates_{target_type}_{timestamp}.csv'
        df_result.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n完整结果已保存至: {output_file}")
        
        # 输出详细分析
        print_detailed_analysis(df_result.head(5))
        
    else:
        print("\n⚠️  未找到符合条件的标的")
        print("建议:")
        print("  1. 降低最低评分要求 (--min-score)")
        print("  2. 扩大筛选范围 (--target-type all)")
        print("  3. 检查市场环境是否适合网格交易")


def get_candidates_list(target_type: str) -> List[tuple]:
    """
    获取候选标的列表
    
    Args:
        target_type: 'etf', 'stock', 'all'
        
    Returns:
        [(code, name), ...]
    """
    candidates = []
    
    if target_type in ['etf', 'all']:
        # ETF列表（常见的流动性好的ETF）
        etf_list = [
            ('510300', '300ETF'),
            ('510500', '500ETF'),
            ('159915', '创业板ETF'),
            ('512000', '券商ETF'),
            ('159992', '创业板ETF'),
            ('159949', '创业板50'),
            ('510050', '50ETF'),
            ('159905', '深红利'),
            ('159901', '深100ETF'),
            ('512100', '中证1000ETF'),
            ('512880', '证券ETF'),
            ('512660', '军工ETF'),
            ('512690', '白酒ETF'),
            ('159928', '消费ETF'),
            ('513050', '中概互联'),
            ('515000', '科创50ETF'),
            ('588000', '科创50ETF'),
            ('512480', '半导体ETF'),
            ('515050', '5GETF'),
            ('516160', '新能源ETF'),
        ]
        candidates.extend(etf_list)
    
    if target_type in ['stock', 'all']:
        # 股票列表（可以从adata获取，这里先用示例）
        try:
            import adata
            # 获取全部A股
            stock_df = adata.stock.info.all_code()
            if stock_df is not None and not stock_df.empty:
                # 随机采样或按市值筛选
                stock_list = [(row['code'], row['name']) 
                             for _, row in stock_df.head(50).iterrows()]
                candidates.extend(stock_list)
        except Exception as e:
            logger.warning(f"获取股票列表失败: {e}")
            # 示例股票
            example_stocks = [
                ('600519', '贵州茅台'),
                ('601318', '中国平安'),
                ('000858', '五粮液'),
            ]
            candidates.extend(example_stocks)
    
    return candidates


def get_historical_data(code: str, years: int) -> Optional[pd.DataFrame]:
    """
    获取历史行情数据
    
    Args:
        code: 股票/ETF代码
        years: 年数
        
    Returns:
        DataFrame或None
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * years + 30)  # 多取一个月
    
    try:
        df = load_or_fetch_kline(
            code=code,
            fetch_func=fetch_kline_from_api,
            start_date=start_date.strftime('%Y-%m-%d')
        )
        return df
    except Exception as e:
        logger.debug(f"获取 {code} 数据失败: {e}")
        return None


def print_detailed_analysis(df: pd.DataFrame):
    """打印详细分析"""
    if df.empty:
        return
    
    print("\n" + "=" * 80)
    print("Top 5 详细分析")
    print("=" * 80)
    
    for i, row in df.iterrows():
        if i >= 5:
            break
        
        print(f"\n【{i+1}】 {row.get('代码', 'N/A')} - {row.get('名称', 'N/A')}")
        print(f"  综合评分: {row.get('网格评分', 0):.0f} 分")
        print(f"  年化波动率: {row.get('年化波动率%', 0):.2f}%")
        print(f"  趋势强度: {row.get('趋势强度', 0):.2f} (越小越横盘)")
        print(f"  日均成交额: {row.get('日均成交额(万)', 0):,.0f} 万元")
        
        # 3月份分析
        march_return = row.get('3月平均收益%', 0)
        march_win = row.get('3月胜率%', 0)
        print(f"  3月份表现: 平均收益 {march_return:+.2f}%, 胜率 {march_win:.0f}%")
        
        # 两会期间分析
        sessions_return = row.get('两会收益%', 0)
        sessions_vol = row.get('两会波动率%', 0)
        sessions_dd = row.get('两会最大回撤%', 0)
        print(f"  两会期间: 收益 {sessions_return:+.2f}%, 波动 {sessions_vol:.2f}%, 回撤 {sessions_dd:.2f}%")
        
        # 当前位置
        position = row.get('当前位置', 0)
        if position <= 0.3:
            pos_desc = "底部区域(适合建仓)"
        elif position >= 0.7:
            pos_desc = "顶部区域(谨慎建仓)"
        else:
            pos_desc = "中部区域"
        print(f"  当前位置: {position:.2f} ({pos_desc})")
        
        # 建议
        suitable = row.get('两会期间适合', False)
        if suitable:
            print(f"  ✓ 适合两会期间网格交易")
        else:
            print(f"  注意: 两会期间波动可能较大")


def print_grid_trading_guide():
    """打印网格交易使用指南"""
    print("\n" + "=" * 80)
    print("网格交易策略指南")
    print("=" * 80)
    
    print("\n【什么是网格交易】")
    print("网格交易是一种机械化的交易策略，在价格区间内设置多个买卖价格网格，")
    print("价格下跌时买入，上涨时卖出，赚取震荡市场的波动收益。")
    
    print("\n【适用场景】")
    print("✓ 震荡市场（横盘整理）")
    print("✓ 波动适中（年化15%-40%）")
    print("✓ 流动性充足")
    print("✓ 无明显单边趋势")
    
    print("\n【3月份和两会特征】")
    print("• 3月份通常是政策窗口期，市场预期较多")
    print("• 两会期间（3月初）市场波动可能增加")
    print("• 历史数据显示，部分板块在两会前后有较好的波动规律")
    print("• 建议：两会前布局，两会期间执行网格策略")
    
    print("\n【风险提示】")
    print("⚠️  单边趋势市会导致亏损（上涨踏空或下跌套牢）")
    print("⚠️  需要足够的资金分配到多个网格")
    print("⚠️  交易成本（手续费、印花税）会侵蚀收益")
    print("⚠️  需要纪律性执行，避免情绪干扰")
    
    print("\n【建议配置】")
    print("• 网格数量: 5-10个")
    print("• 价格区间: 基于支撑位和阻力位设置")
    print("• 单格资金: 总资金的10%-20%")
    print("• 止损位: 区间下方5%-10%")
    print("• 止盈位: 区间上方设置或不设")
    
    print("\n" + "=" * 80)
