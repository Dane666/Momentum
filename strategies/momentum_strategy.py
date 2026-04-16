# -*- coding: utf-8 -*-
from .base_strategy import BaseStrategy
from ..core.engine import MomentumEngine
from ..backtest import run_sensitivity_analysis
import pandas as pd
import logging

logger = logging.getLogger('momentum.strategy')

class MomentumStrategy(BaseStrategy):
    """
    基于 MomentumEngine 的封装策略
    复用原有核心逻辑，适配 BaseStrategy 接口
    """
    def __init__(self, config=None):
        super().__init__("MomentumV16", config)
        # 默认观察列表和持仓可以从 config 或外部传入
        watchlist = config.get('watchlist', []) if config else []
        holdings = config.get('holdings', []) if config else []
        self.engine = MomentumEngine(watchlist=watchlist, holdings=holdings)

    def on_bar(self, bar):
        """
        MomentumEngine 目前主要是批处理模式，on_bar 不直接适用。
        如果未来支持事件驱动，可以在此调用 engine.update(bar)
        """
        pass

    def scan(self, watchlist: list = None) -> pd.DataFrame:
        """
        调用 MomentumEngine 的扫描逻辑
        """
        logger.info("Starting Momentum Strategy Scan...")
        # 如果传入了新的 watchlist，更新 engine
        if watchlist:
            self.engine.watchlist = watchlist
        
        # 使用 run_all_market_scan_pro 进行全市场扫描
        # 注意：run_all_market_scan_pro 返回 (df, report_text)
        result, report = self.engine.run_all_market_scan_pro()
        return result

    def run_backtest(self, data: pd.DataFrame = None, **kwargs):
        """
        运行回测
        """
        logger.info("Starting Momentum Strategy Backtest...")
        days = kwargs.get('days', 250)
        periods = kwargs.get('periods', [3, 4])
        
        # 调用现有的敏感性分析回测
        # 注意：run_sensitivity_analysis 是一个独立函数，不依赖 engine 实例
        run_sensitivity_analysis(days=days, periods=periods)
