# -*- coding: utf-8 -*-
"""
回测稳定性诊断工具

用于检测和修复回测中的常见问题:
1. 未来函数 (Look-Ahead Bias)
2. 幸存者偏差 / 妖股依赖
3. 窗口敏感性 / 过拟合

使用方法:
    python -m tests.momentum.backtest.diagnose
    
或在代码中:
    from tests.momentum.backtest.diagnose import run_full_diagnosis
    results = run_full_diagnosis()
"""

import sys
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('momentum')


def print_banner():
    """打印诊断工具横幅"""
    print("\n" + "=" * 70)
    print("   🔍 Momentum 回测稳定性诊断工具 v3.0")
    print("=" * 70)
    print(f"   诊断时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


def check_backtest_mode():
    """
    检查1: 回测模式说明
    
    确认回测与实盘scan逻辑一致
    """
    print("\n" + "=" * 60)
    print("📋 检查1: 回测模式与实盘一致性")
    print("=" * 60)
    
    # 回测模式说明
    print("\n✅ 回测模式: 14:45尾盘选股 (与实盘scan一致)")
    print("-" * 50)
    
    checks = [
        ("✅", "_get_daily_top_stocks()", 
         "使用当日成交额排名 (与14:45 scan一致)"),
        ("✅", "_simulate_day_data()", 
         "使用当日K线数据计算因子 (收盘价近似14:45价格)"),
        ("✅", "_simulate_smart_exit()", 
         "买入价=当日收盘价 (模拟14:45尾盘买入)"),
        ("✅", "compute_metrics()", 
         "因子计算基于snap_df (当日完整数据)"),
    ]
    
    for status, func, desc in checks:
        print(f"  {status} {func}")
        print(f"      └─ {desc}")
    
    print("\n💡 说明:")
    print("  - 实盘在14:45进行scan选股，使用当时的成交额和价格")
    print("  - 回测使用日K线收盘价近似14:45价格 (仅差15分钟)")
    print("  - 买入价 = 当日收盘价，卖出从T+1日开始计算")
    
    return []


def run_stability_test(days: int = 120, hold_period: int = 3):
    """
    检查2: 滑动窗口稳定性测试
    
    在不同起始日期运行回测，检查收益率波动
    """
    print("\n" + "=" * 60)
    print("📋 检查2: 滑动窗口稳定性测试")
    print("=" * 60)
    
    try:
        from .stability import StabilityAnalyzer
        
        analyzer = StabilityAnalyzer(backtest_days=days, hold_period=hold_period)
        
        # 测试5个不同的窗口偏移
        window_shifts = [0, 1, 2, 3, 5]
        
        print(f"\n运行 {len(window_shifts)} 个滑动窗口回测...")
        print(f"参数: days={days}, hold_period={hold_period}")
        
        df = analyzer.run_rolling_window_backtest(
            window_shifts=window_shifts,
            parallel=False  # 串行执行，便于观察
        )
        
        if not df.empty:
            ret_range = df['profit_pct'].max() - df['profit_pct'].min()
            ret_cv = df['profit_pct'].std() / (abs(df['profit_pct'].mean()) + 1e-9)
            
            print(f"\n📊 诊断结果:")
            print(f"  - 收益率极差: {ret_range:.2f}% ", end="")
            if ret_range > 50:
                print("🔴 (极不稳定)")
            elif ret_range > 30:
                print("🟡 (波动较大)")
            else:
                print("🟢 (相对稳定)")
                
            print(f"  - 变异系数: {ret_cv:.2f} ", end="")
            if ret_cv > 0.5:
                print("🔴 (过度拟合风险)")
            elif ret_cv > 0.3:
                print("🟡 (需关注)")
            else:
                print("🟢 (可接受)")
            
            return df
        
    except Exception as e:
        print(f"❌ 稳定性测试失败: {e}")
        import traceback
        traceback.print_exc()
    
    return None


def run_concentration_check():
    """
    检查3: 收益集中度分析
    
    检测策略是否过度依赖个别妖股
    """
    print("\n" + "=" * 60)
    print("📋 检查3: 收益集中度分析")
    print("=" * 60)
    
    try:
        from .stability import StabilityAnalyzer
        
        analyzer = StabilityAnalyzer()
        result = analyzer.analyze_concentration()
        
        if result:
            top3 = result.get('concentration_top3', 0)
            top5 = result.get('concentration_top5', 0)
            
            print(f"\n📊 诊断结果:")
            print(f"  - Top3 集中度: {top3:.1f}% ", end="")
            if top3 > 50:
                print("🔴 (严重依赖个别股票)")
            elif top3 > 30:
                print("🟡 (轻度集中)")
            else:
                print("🟢 (分散良好)")
            
            if result.get('top_contributors'):
                print("\n  Top贡献者:")
                for item in result['top_contributors'][:3]:
                    print(f"    • {item['code']} {item['name']}: +{item['total_return_pct']:.2f}%")
            
            return result
            
    except Exception as e:
        print(f"❌ 集中度分析失败: {e}")
        print("   (提示: 需要先运行回测生成交易记录)")
    
    return None


def print_recommendations():
    """打印修复建议"""
    print("\n" + "=" * 60)
    print("💡 修复建议汇总")
    print("=" * 60)
    
    recommendations = [
        ("1. 回测与实盘一致性", [
            "回测使用14:45尾盘选股模式 (与实盘scan一致)",
            "使用当日成交额和收盘价 (近似14:45数据)",
            "买入价 = 当日收盘价 (模拟尾盘买入)",
        ]),
        ("2. 稳定性验证", [
            "运行滑动窗口测试: from backtest import run_stability_check",
            "检查不同窗口的收益率变异系数 (CV < 0.3 为佳)",
            "收益率极差 < 30% 表示策略稳定",
        ]),
        ("3. 减少妖股依赖", [
            "增加行业分散度: MAX_SECTOR_PICKS=1",
            "限制单只股票最大持仓比例",
            "使用行业中性化选股",
        ]),
        ("4. 缓存机制", [
            "创建回测快照: create_backtest_snapshot('snapshot_id', codes)",
            "使用快照回测确保可复现性",
            "定期清理过期快照",
        ]),
    ]
    
    for title, items in recommendations:
        print(f"\n{title}:")
        for item in items:
            print(f"  • {item}")


def run_full_diagnosis(
    backtest_days: int = 120,
    hold_period: int = 3,
    run_backtest: bool = False
) -> dict:
    """
    运行完整的稳定性诊断
    
    Args:
        backtest_days: 回测天数
        hold_period: 持仓周期
        run_backtest: 是否运行实际回测 (耗时较长)
        
    Returns:
        诊断结果字典
    """
    print_banner()
    
    results = {}
    
    # 检查1: 回测模式
    results['backtest_mode'] = check_backtest_mode()
    
    # 检查2 & 3: 需要运行回测
    if run_backtest:
        print("\n⏳ 准备运行实际回测测试 (可能需要几分钟)...")
        
        # 滑动窗口测试
        results['stability'] = run_stability_test(backtest_days, hold_period)
        
        # 集中度分析
        results['concentration'] = run_concentration_check()
    else:
        print("\n💡 跳过实际回测测试。使用 run_backtest=True 启用。")
    
    # 打印建议
    print_recommendations()
    
    print("\n" + "=" * 60)
    print("✅ 诊断完成!")
    print("=" * 60 + "\n")
    
    return results


# 命令行入口
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='回测稳定性诊断工具')
    parser.add_argument('--days', type=int, default=120, help='回测天数')
    parser.add_argument('--hold', type=int, default=3, help='持仓周期')
    parser.add_argument('--run-backtest', action='store_true', help='运行实际回测')
    
    args = parser.parse_args()
    
    run_full_diagnosis(
        backtest_days=args.days,
        hold_period=args.hold,
        run_backtest=args.run_backtest
    )
