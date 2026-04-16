# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
import pandas as pd
import logging

logger = logging.getLogger('momentum.strategy')

class BaseStrategy(ABC):
    """
    量化策略基类
    定义了策略的标准接口，支持回测、实盘扫描和信号生成。
    """
    def __init__(self, name: str, config: dict = None):
        self.name = name
        self.config = config or {}
        self.positions = {}
        self.logger = logger

    @abstractmethod
    def on_bar(self, bar):
        """
        K线更新时调用 (事件驱动模式)
        :param bar: dict or Series, 包含 date, open, high, low, close, volume 等
        """
        pass

    @abstractmethod
    def scan(self, watchlist: list = None) -> pd.DataFrame:
        """
        市场扫描模式：对给定列表或全市场进行扫描，返回符合条件的标的
        :param watchlist: 待扫描的股票代码列表
        :return: 包含信号和因子的 DataFrame
        """
        pass

    def run_backtest(self, data: pd.DataFrame, **kwargs):
        """
        运行回测
        :param data: 回测数据
        :param kwargs: 其他回测参数
        """
        print(f"Running backtest for strategy: {self.name}")
        # 子类可以覆盖此方法以集成具体的回测逻辑
        pass

    def get_positions(self):
        return self.positions
