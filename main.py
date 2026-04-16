# -*- coding: utf-8 -*-
"""
Momentum v16 - 量化策略主入口

功能模块:
- 持仓诊断 (股票 + ETF)
- 市场扫描选股
- 策略回测
- 可视化分析

架构说明:
- cli/monitor.py: 持仓诊断相关命令
- cli/backtest_cmd.py: 回测相关命令
- cli/analysis.py: 分析报告相关命令

使用示例:
  python main.py --mode scan        # 市场扫描选股
  python main.py --mode backtest    # 策略回测 (自动生成报告)
  python main.py --mode explain     # 显示策略规则
"""

import sys
import os
import warnings
import logging
import argparse
from datetime import datetime
from typing import List, Dict

# ==================== 全局初始化 ====================
# 1. 过滤资源警告
warnings.simplefilter("ignore", ResourceWarning)

# 2. 全局禁用代理
PROXY_KEYS = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 
              'all_proxy', 'ALL_PROXY', 'ftp_proxy', 'FTP_PROXY']
for key in PROXY_KEYS:
    os.environ.pop(key, None)
os.environ['no_proxy'] = '*'
os.environ['NO_PROXY'] = '*'

# 添加父目录到路径 (用于 from momentum import ...)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 添加项目根目录到路径 (用于 import adata)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 配置日志
from momentum import config as cfg

logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(cfg.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stderr)
    ]
)
logging.getLogger().handlers[1].setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("efinance").setLevel(logging.ERROR)

logger = logging.getLogger('momentum')


# ==================== 持仓配置 ====================
class PortfolioConfig:
    """持仓配置类 - 在此配置你的持仓和观察清单"""
    
    # 股票持仓 (格式: {'代码': 买入价}，用于计算止损位)
    HOLDINGS: Dict[str, float] = {   # 买入价
        '600570' : 30.873
    }
    
    # ETF 持仓
    ETF_HOLDINGS: List[str] = ['515120', '513650', '159995','159227', '513310']
    
    # 观察清单
    WATCHLIST: List[str] = list(HOLDINGS.keys())


# ==================== 命令行解析 ====================
def create_parser() -> argparse.ArgumentParser:
    """创建命令行解析器"""
    parser = argparse.ArgumentParser(
        description='Momentum 量化策略',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
运行示例:
  python main.py --mode scan        # 市场扫描选股
  python main.py --mode monitor     # 持仓诊断
  python main.py --mode all         # 完整流程
  python main.py --mode all_ollama  # 完整流程 + Ollama AI 分析
  python main.py --mode etf_scan    # ETF 行业轮动扫描
  python main.py --mode backtest    # 策略回测 (自动生成报告)
  python main.py --mode backtest --no-report  # 仅回测
  python main.py --mode optimize    # 参数优化 (遍历止盈止损等参数)
  python main.py --mode explain     # 显示策略规则
  python main.py --mode grid        # 网格交易标的筛选
  python main.py --mode grid_guide  # 网格交易策略指南
  
独立查看历史回测:
  python main.py --mode history     # 查看回测历史
  python main.py --mode visualize   # 可视化最近回测
  python main.py --mode analyze     # 交易原因分析

建议执行时间:
  - monitor:  14:10 盘中持仓诊断
  - scan:     14:40 尾盘选股
  - backtest: 策略回测，自动生成可视化报告
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['scan', 'monitor', 'all', 'all_ollama', 'etf_scan', 
                 'backtest', 'optimize', 'history', 'visualize', 'analyze', 'explain',
                 'grid', 'grid_guide'],
        default='scan',
        help='运行模式'
    )
    parser.add_argument('--days', type=int, default=250, help='回测天数')
    parser.add_argument('--periods', type=str, default='3,4', help='持仓周期列表')
    parser.add_argument('--gemini', action='store_true', help='启用 Gemini AI')
    parser.add_argument('--ollama-model', type=str, default='qwen3:14b', help='Ollama 模型')
    parser.add_argument('--session', type=str, default=None, help='回测会话ID')
    parser.add_argument('--no-record', action='store_true', help='不记录交易')
    parser.add_argument('--no-report', action='store_true', help='不生成报告')
    parser.add_argument('--fast', action='store_true', help='参数优化快速模式')
    parser.add_argument('--metric', type=str, default='sharpe', choices=['sharpe', 'profit', 'calmar'], help='优化目标指标')
    parser.add_argument('--save-dir', type=str, default='./reports', help='报告目录')
    parser.add_argument('--target-type', type=str, default='etf', choices=['etf', 'stock', 'all'], help='网格筛选标的类型')
    parser.add_argument('--min-score', type=int, default=60, help='网格筛选最低评分')
    parser.add_argument('--top-n', type=int, default=20, help='显示前N个结果')
    
    return parser


# ==================== 命令路由 ====================
def dispatch(args: argparse.Namespace):
    """根据参数分发到对应的命令处理函数"""
    from momentum.cli import (
        run_portfolio_monitor,
        run_market_scan,
        run_full_workflow,
        run_etf_scan,
        run_full_workflow_with_ollama,
        run_backtest,
        show_backtest_history,
        show_session_detail,
        run_visualize,
        run_trade_analysis,
        show_strategy_rules,
        run_grid_screening,
        print_grid_trading_guide,
    )
    
    config = PortfolioConfig()
    
    logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"运行模式: {args.mode}")
    logger.info(f"持仓列表: {config.HOLDINGS}")
    
    # ========== 持仓诊断类 ==========
    if args.mode == 'scan':
        run_market_scan(
            holdings=config.HOLDINGS,
            watchlist=config.WATCHLIST,
            enable_gemini=args.gemini
        )
        
    elif args.mode == 'monitor':
        run_portfolio_monitor(
            holdings=config.HOLDINGS,
            etf_holdings=config.ETF_HOLDINGS,
            watchlist=config.WATCHLIST,
            enable_gemini=args.gemini
        )
        
    elif args.mode == 'all':
        run_full_workflow(
            holdings=config.HOLDINGS,
            etf_holdings=config.ETF_HOLDINGS,
            watchlist=config.WATCHLIST,
            enable_gemini=args.gemini
        )
        
    elif args.mode == 'all_ollama':
        run_full_workflow_with_ollama(
            holdings=config.HOLDINGS,
            etf_holdings=config.ETF_HOLDINGS,
            watchlist=config.WATCHLIST,
            enable_gemini=args.gemini,
            ollama_model=args.ollama_model
        )
        
    elif args.mode == 'etf_scan':
        run_etf_scan(
            holdings=config.HOLDINGS,
            watchlist=config.WATCHLIST,
            enable_gemini=args.gemini
        )
    
    # ========== 回测类 ==========
    elif args.mode == 'backtest':
        periods = [int(p.strip()) for p in args.periods.split(',')]
        run_backtest(
            days=args.days,
            periods=periods,
            record_trades=not args.no_record,
            auto_report=not args.no_report,
            save_dir=args.save_dir
        )
    
    elif args.mode == 'optimize':
        from momentum.backtest import run_param_optimization
        best_config = run_param_optimization(
            days=args.days,
            fast_mode=args.fast,
            metric=args.metric,
            save_results=True,
        )
        if best_config:
            print("\n💡 将最优参数写入 config.py:")
            for key, value in best_config.items():
                print(f"   {key} = {value}")
        
    elif args.mode == 'history':
        if args.session:
            show_session_detail(args.session)
        else:
            show_backtest_history()
            
    elif args.mode == 'visualize':
        run_visualize(session_id=args.session, save_dir=args.save_dir)
    
    # ========== 分析类 ==========
    elif args.mode == 'analyze':
        run_trade_analysis(session_id=args.session, save_dir=args.save_dir)
        
    elif args.mode == 'explain':
        show_strategy_rules()
    
    # ========== 网格交易类 ==========
    elif args.mode == 'grid':
        run_grid_screening(
            target_type=args.target_type,
            lookback_years=3,
            min_score=args.min_score,
            top_n=args.top_n
        )
    
    elif args.mode == 'grid_guide':
        print_grid_trading_guide()


def main():
    """主入口"""
    parser = create_parser()
    args = parser.parse_args()
    dispatch(args)


if __name__ == "__main__":
    main()

