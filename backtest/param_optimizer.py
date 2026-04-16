# -*- coding: utf-8 -*-
"""
参数优化器 - 遍历回测参数寻找最优配置

支持优化的参数:
- 持仓周期 (hold_period)
- ATR止损系数 (atr_stop_factor)
- 固定止损百分比 (fixed_stop_pct)
- 固定止盈百分比 (take_profit_pct)
- 乖离率止盈阈值 (bias_profit_limit)
- RSI超买阈值 (rsi_danger_zone)
- 最小夏普阈值 (min_sharpe)

输出:
- 参数组合排名表 (按夏普比率/收益排序)
- 最优参数推荐
- 参数敏感性热力图
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from itertools import product
from tqdm import tqdm
import logging
import json

logger = logging.getLogger('momentum')


# ==================== 参数搜索空间 ====================
DEFAULT_PARAM_GRID = {
    # 持仓周期 (天)
    'hold_period': [2, 3, 4, 5],
    
    # 每期最大持仓数量 (关键参数)
    'max_total_picks': [1, 2, 3, 4, 5],
    
    # 同行业最大持仓数
    'max_sector_picks': [1, 2, 3],
    
    # ATR止损系数 (越大止损越宽松)
    'atr_stop_factor': [0.8, 1.0, 1.2, 1.5, 2.0],
    
    # 固定止损百分比
    'fixed_stop_pct': [0.03, 0.05, 0.07, 0.10],
    
    # 固定止盈百分比
    'take_profit_pct': [0.08, 0.10, 0.15, 0.20],
    
    # 乖离率止盈阈值
    'bias_profit_limit': [0.15, 0.20, 0.25],
    
    # RSI超买阈值
    'rsi_danger_zone': [75.0, 80.0, 85.0],
    
    # 最小夏普比率 (选股过滤)
    'min_sharpe': [0.5, 1.0, 1.5],
    
    # 自适应止损开关 (True=动态调整止盈止损, False=固定止盈止损)
    'use_adaptive_exit': [False, True],
}

# 快速模式参数网格 (减少搜索空间)
FAST_PARAM_GRID = {
    'hold_period': [3, 4, 5],
    'max_total_picks': [1, 2, 3, 4],  # 核心: 测试个股数量
    'max_sector_picks': [1, 2],
    'atr_stop_factor': [1.2, 1.5],
    'fixed_stop_pct': [0.05],
    'take_profit_pct': [0.10],
    'use_adaptive_exit': [False, True],  # 对比固定 vs 自适应
    'enable_manipulation_filter': [False, True],  # 对比是否启用庄股过滤
}

# 动量策略推荐参数范围
MOMENTUM_RECOMMENDED = {
    'hold_period': (3, 5),        # 动量效应3-5天最强
    'max_total_picks': (1, 4),    # 持仓1-4只
    'max_sector_picks': (1, 2),   # 同行业1-2只
    'atr_stop_factor': (1.0, 1.5), # ATR止损1-1.5倍
    'fixed_stop_pct': (0.05, 0.08), # 止损5-8%
    'take_profit_pct': (0.10, 0.20), # 止盈10-20%
    'bias_profit_limit': (0.15, 0.25), # 乖离率15-25%
    'rsi_danger_zone': (75.0, 85.0),  # RSI超买75-85
    'min_sharpe': (0.8, 1.5),     # 夏普过滤0.8-1.5
}


class ParamOptimizer:
    """
    回测参数优化器
    
    通过网格搜索遍历参数组合，找到最优配置。
    """
    
    def __init__(
        self,
        backtest_days: int = 120,
        param_grid: Optional[Dict[str, List]] = None,
        fast_mode: bool = True,
        metric: str = 'sharpe',  # 优化目标: sharpe, profit, calmar
    ):
        """
        初始化参数优化器
        
        Args:
            backtest_days: 回测天数
            param_grid: 参数搜索空间 (默认使用快速模式)
            fast_mode: 快速模式 (减少搜索空间)
            metric: 优化目标指标
        """
        self.backtest_days = backtest_days
        self.metric = metric
        
        if param_grid is not None:
            self.param_grid = param_grid
        elif fast_mode:
            self.param_grid = FAST_PARAM_GRID.copy()
        else:
            self.param_grid = DEFAULT_PARAM_GRID.copy()
        
        self.results: List[Dict] = []
        self.best_params: Optional[Dict] = None
        
    def _count_combinations(self) -> int:
        """计算参数组合总数"""
        total = 1
        for values in self.param_grid.values():
            total *= len(values)
        return total
    
    def _generate_combinations(self) -> List[Dict]:
        """生成所有参数组合"""
        keys = list(self.param_grid.keys())
        values = list(self.param_grid.values())
        
        combinations = []
        for combo in product(*values):
            param_dict = dict(zip(keys, combo))
            combinations.append(param_dict)
        
        return combinations
    
    def _run_single_backtest(self, params: Dict) -> Optional[Dict]:
        """
        运行单次回测
        
        Args:
            params: 参数字典
            
        Returns:
            回测结果字典
        """
        from .simulator import MomentumBacktester
        from ..risk import ExitConfig
        
        try:
            # 提取持仓周期
            hold_period = params.get('hold_period', 3)
            
            # 构建退出规则配置
            exit_config = ExitConfig(
                fixed_stop_pct=params.get('fixed_stop_pct', 0.05),
                take_profit_pct=params.get('take_profit_pct', 0.10),
                bias_profit_limit=params.get('bias_profit_limit', 0.20),
                rsi_danger_zone=params.get('rsi_danger_zone', 80.0),
            )
            
            # 临时修改全局配置
            from .. import config as cfg
            original_atr_stop = getattr(cfg, 'ATR_STOP_FACTOR', 1.2)
            original_min_sharpe = getattr(cfg, 'MIN_SHARPE', 1.0)
            original_fixed_stop = getattr(cfg, 'FIXED_STOP_PCT', 0.05)
            original_take_profit = getattr(cfg, 'TAKE_PROFIT_PCT', 0.10)
            original_bias_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20)
            original_rsi_danger = getattr(cfg, 'RSI_DANGER_ZONE', 80.0)
            
            # 应用新参数
            cfg.ATR_STOP_FACTOR = params.get('atr_stop_factor', 1.2)
            cfg.MIN_SHARPE = params.get('min_sharpe', 1.0)
            cfg.FIXED_STOP_PCT = params.get('fixed_stop_pct', 0.05)
            cfg.TAKE_PROFIT_PCT = params.get('take_profit_pct', 0.10)
            cfg.BIAS_PROFIT_LIMIT = params.get('bias_profit_limit', 0.20)
            cfg.RSI_DANGER_ZONE = params.get('rsi_danger_zone', 80.0)
            
            # 创建回测器 (禁用交易记录以加速)
            tester = MomentumBacktester(
                backtest_days=self.backtest_days,
                hold_period=hold_period,
                record_trades=False,  # 禁用交易记录加速
                use_1445_data=False,  # 使用日K数据加速
            )
            
            # 执行回测
            result = tester.run_backtest()
            
            # 恢复原始配置
            cfg.ATR_STOP_FACTOR = original_atr_stop
            cfg.MIN_SHARPE = original_min_sharpe
            cfg.FIXED_STOP_PCT = original_fixed_stop
            cfg.TAKE_PROFIT_PCT = original_take_profit
            cfg.BIAS_PROFIT_LIMIT = original_bias_limit
            cfg.RSI_DANGER_ZONE = original_rsi_danger
            
            if result:
                return {
                    **params,
                    'profit_pct': result.get('profit_pct', 0),
                    'annual_ret': result.get('annual_ret', 0),
                    'sharpe': result.get('sharpe', 0),
                    'max_dd': result.get('max_dd', 0),
                    'win_rate': result.get('win_rate', 0),
                    'trade_count': result.get('trade_count', 0),
                    'calmar': result.get('annual_ret', 0) / max(result.get('max_dd', 1), 0.1),
                }
            
        except Exception as e:
            logger.warning(f"[Optimizer] 参数组合回测失败: {params}, 错误: {e}")
        
        return None
    
    def optimize(self, show_progress: bool = True) -> pd.DataFrame:
        """
        执行参数优化
        
        Args:
            show_progress: 是否显示进度条
            
        Returns:
            排序后的结果 DataFrame
        """
        combinations = self._generate_combinations()
        total = len(combinations)
        
        print(f"\n{'█' * 70}")
        print(f"🔍 参数优化开始")
        print(f"  回测天数: {self.backtest_days}")
        print(f"  参数组合: {total}")
        print(f"  优化目标: {self.metric}")
        print(f"{'█' * 70}\n")
        
        self.results = []
        
        # 预加载数据 (只需加载一次)
        print("📦 预加载市场数据...")
        from .simulator import MomentumBacktester
        preloader = MomentumBacktester(
            backtest_days=self.backtest_days,
            hold_period=3,
            record_trades=False,
            use_1445_data=False,
        )
        preloader.prepare_backtest_data()
        
        # 共享缓存
        shared_cache = {
            'all_data_cache': preloader.all_data_cache,
            'sector_cache': preloader.sector_cache,
            'stock_info_cache': preloader.stock_info_cache,
            'code_names': getattr(preloader, 'code_names', {}),
        }
        
        print(f"\n🚀 开始遍历 {total} 个参数组合...\n")
        
        iterator = tqdm(combinations, desc="参数优化") if show_progress else combinations
        
        for params in iterator:
            result = self._run_single_backtest_with_cache(params, shared_cache)
            if result:
                self.results.append(result)
        
        if not self.results:
            print("⚠️ 所有参数组合回测均失败")
            return pd.DataFrame()
        
        # 转换为 DataFrame 并排序
        df = pd.DataFrame(self.results)
        
        # 按优化目标排序
        if self.metric == 'sharpe':
            df = df.sort_values('sharpe', ascending=False)
        elif self.metric == 'profit':
            df = df.sort_values('profit_pct', ascending=False)
        elif self.metric == 'calmar':
            df = df.sort_values('calmar', ascending=False)
        
        # 记录最优参数
        if not df.empty:
            self.best_params = df.iloc[0].to_dict()
        
        return df
    
    def _run_single_backtest_with_cache(
        self, 
        params: Dict, 
        shared_cache: Dict
    ) -> Optional[Dict]:
        """
        使用共享缓存运行单次回测
        
        Args:
            params: 参数字典
            shared_cache: 共享的数据缓存
            
        Returns:
            回测结果字典
        """
        from .simulator import MomentumBacktester
        
        try:
            # 提取持仓周期
            hold_period = params.get('hold_period', 3)
            
            # 临时修改全局配置
            from .. import config as cfg
            original_values = {
                'ATR_STOP_FACTOR': getattr(cfg, 'ATR_STOP_FACTOR', 1.2),
                'MIN_SHARPE': getattr(cfg, 'MIN_SHARPE', 1.0),
                'FIXED_STOP_PCT': getattr(cfg, 'FIXED_STOP_PCT', 0.05),
                'TAKE_PROFIT_PCT': getattr(cfg, 'TAKE_PROFIT_PCT', 0.10),
                'BIAS_PROFIT_LIMIT': getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20),
                'RSI_DANGER_ZONE': getattr(cfg, 'RSI_DANGER_ZONE', 80.0),
                'MAX_TOTAL_PICKS': getattr(cfg, 'MAX_TOTAL_PICKS', 4),
                'MAX_SECTOR_PICKS': getattr(cfg, 'MAX_SECTOR_PICKS', 1),
                'USE_ADAPTIVE_EXIT': getattr(cfg, 'USE_ADAPTIVE_EXIT', False),
                'ENABLE_MANIPULATION_FILTER': getattr(cfg, 'ENABLE_MANIPULATION_FILTER', False),
            }
            
            # 应用新参数
            cfg.ATR_STOP_FACTOR = params.get('atr_stop_factor', 1.2)
            cfg.MIN_SHARPE = params.get('min_sharpe', 1.0)
            cfg.FIXED_STOP_PCT = params.get('fixed_stop_pct', 0.05)
            cfg.TAKE_PROFIT_PCT = params.get('take_profit_pct', 0.10)
            cfg.BIAS_PROFIT_LIMIT = params.get('bias_profit_limit', 0.20)
            cfg.RSI_DANGER_ZONE = params.get('rsi_danger_zone', 80.0)
            cfg.MAX_TOTAL_PICKS = params.get('max_total_picks', 4)
            cfg.MAX_SECTOR_PICKS = params.get('max_sector_picks', 1)
            cfg.USE_ADAPTIVE_EXIT = params.get('use_adaptive_exit', False)
            cfg.ENABLE_MANIPULATION_FILTER = params.get('enable_manipulation_filter', False)
            
            # 创建回测器
            tester = MomentumBacktester(
                backtest_days=self.backtest_days,
                hold_period=hold_period,
                record_trades=False,
                use_1445_data=False,
            )
            
            # 复用共享缓存 (避免重复加载数据)
            tester.all_data_cache = shared_cache['all_data_cache']
            tester.sector_cache = shared_cache['sector_cache']
            tester.stock_info_cache = shared_cache['stock_info_cache']
            tester.code_names = shared_cache['code_names']
            
            # 执行回测 (跳过数据加载)
            result = tester._run_backtest_with_cache()
            
            # 恢复原始配置
            for key, value in original_values.items():
                setattr(cfg, key, value)
            
            if result:
                return {
                    **params,
                    'profit_pct': result.get('profit_pct', 0),
                    'annual_ret': result.get('annual_ret', 0),
                    'sharpe': result.get('sharpe', 0),
                    'max_dd': result.get('max_dd', 0),
                    'win_rate': result.get('win_rate', 0),
                    'trade_count': result.get('trade_count', 0),
                    'calmar': result.get('annual_ret', 0) / max(result.get('max_dd', 1), 0.1),
                }
            
        except Exception as e:
            logger.debug(f"[Optimizer] 参数回测失败: {e}")
        
        return None
    
    def print_report(self, top_n: int = 10):
        """
        打印优化报告
        
        Args:
            top_n: 显示前N个结果
        """
        if not self.results:
            print("⚠️ 没有优化结果")
            return
        
        df = pd.DataFrame(self.results)
        
        # 按夏普排序
        df_sorted = df.sort_values('sharpe', ascending=False)
        
        print(f"\n{'█' * 90}")
        print(f"📊 参数优化报告 (回测天数: {self.backtest_days})")
        print(f"{'█' * 90}")
        
        # 显示前N个结果
        print(f"\n🏆 Top {min(top_n, len(df_sorted))} 参数组合 (按夏普比率排序):")
        print("-" * 90)
        
        # 动态构建表头
        param_cols = [k for k in self.param_grid.keys()]
        header = f"{'排名':<4}"
        for col in param_cols:
            header += f" {col[:12]:<12}"
        header += f" {'收益%':<8} {'年化%':<8} {'夏普':<6} {'回撤%':<7} {'胜率%':<7} {'交易':<5}"
        print(header)
        print("-" * 90)
        
        for i, (_, row) in enumerate(df_sorted.head(top_n).iterrows(), 1):
            line = f"{i:<4}"
            for col in param_cols:
                val = row.get(col, '-')
                if isinstance(val, float):
                    line += f" {val:<12.2f}"
                else:
                    line += f" {val:<12}"
            line += f" {row['profit_pct']:<8.2f} {row['annual_ret']:<8.2f} {row['sharpe']:<6.2f}"
            line += f" {row['max_dd']:<7.2f} {row['win_rate']:<7.1f} {int(row['trade_count']):<5}"
            print(line)
        
        # 最优参数推荐
        if self.best_params:
            print(f"\n{'=' * 90}")
            print(f"🎯 最优参数推荐:")
            print(f"{'=' * 90}")
            for key in param_cols:
                val = self.best_params.get(key, '-')
                print(f"  {key}: {val}")
            print(f"\n  📈 收益: {self.best_params.get('profit_pct', 0):.2f}%")
            print(f"  📊 夏普: {self.best_params.get('sharpe', 0):.2f}")
            print(f"  📉 回撤: {self.best_params.get('max_dd', 0):.2f}%")
            print(f"{'=' * 90}")
        
        # 参数敏感性分析
        self._print_sensitivity_analysis(df)
    
    def _print_sensitivity_analysis(self, df: pd.DataFrame):
        """打印参数敏感性分析"""
        print(f"\n📈 参数敏感性分析:")
        print("-" * 60)
        
        for param in self.param_grid.keys():
            if param not in df.columns:
                continue
            
            grouped = df.groupby(param)['sharpe'].mean()
            best_val = grouped.idxmax()
            best_sharpe = grouped.max()
            
            print(f"  {param}:")
            for val in sorted(df[param].unique()):
                avg_sharpe = grouped.get(val, 0)
                bar = "█" * int(avg_sharpe * 5) if avg_sharpe > 0 else ""
                marker = " ⭐" if val == best_val else ""
                print(f"    {val:>8} → 夏普 {avg_sharpe:>6.2f} {bar}{marker}")
        
        print("-" * 60)
    
    def save_results(self, filepath: str = "param_optimization_results.csv"):
        """保存优化结果到CSV"""
        if self.results:
            df = pd.DataFrame(self.results)
            df = df.sort_values('sharpe', ascending=False)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"✅ 结果已保存: {filepath}")
    
    def get_best_config(self) -> Dict:
        """获取最优配置 (可直接用于更新 config.py)"""
        if not self.best_params:
            return {}
        
        config_update = {
            'HOLD_PERIOD_DEFAULT': self.best_params.get('hold_period', 3),
            'ATR_STOP_FACTOR': self.best_params.get('atr_stop_factor', 1.2),
            'FIXED_STOP_PCT': self.best_params.get('fixed_stop_pct', 0.05),
            'TAKE_PROFIT_PCT': self.best_params.get('take_profit_pct', 0.10),
            'BIAS_PROFIT_LIMIT': self.best_params.get('bias_profit_limit', 0.20),
            'RSI_DANGER_ZONE': self.best_params.get('rsi_danger_zone', 80.0),
            'MIN_SHARPE': self.best_params.get('min_sharpe', 1.0),
            'MAX_TOTAL_PICKS': self.best_params.get('max_total_picks', 4),
            'MAX_SECTOR_PICKS': self.best_params.get('max_sector_picks', 1),
            'USE_ADAPTIVE_EXIT': self.best_params.get('use_adaptive_exit', False),
        }
        return config_update


def run_param_optimization(
    days: int = 120,
    fast_mode: bool = True,
    metric: str = 'sharpe',
    save_results: bool = True,
) -> Dict:
    """
    执行参数优化
    
    Args:
        days: 回测天数
        fast_mode: 快速模式 (减少搜索空间)
        metric: 优化目标 (sharpe/profit/calmar)
        save_results: 是否保存结果
        
    Returns:
        最优参数配置
    """
    optimizer = ParamOptimizer(
        backtest_days=days,
        fast_mode=fast_mode,
        metric=metric,
    )
    
    results = optimizer.optimize()
    optimizer.print_report()
    
    if save_results and not results.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        optimizer.save_results(f"param_optimization_{timestamp}.csv")
    
    return optimizer.get_best_config()
