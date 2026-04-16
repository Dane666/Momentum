# -*- coding: utf-8 -*-
"""
Monitor - 持仓监控模块

职责:
- 实时持仓诊断
- 止盈止损判定
- 动能衰减预警

从 engine.py 提取，遵循单一职责原则。
"""

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Tuple
import logging

logger = logging.getLogger('momentum.monitor')


class PortfolioMonitor:
    """
    持仓监控器
    
    执行持仓诊断流程:
    1. 获取实时行情
    2. 计算持仓因子
    3. 风格中性化
    4. NLP情绪分析 (仅持仓)
    5. 止盈止损判定
    """
    
    def __init__(self, engine):
        """
        初始化监控器
        
        Args:
            engine: MomentumEngine 实例
        """
        self.engine = engine
    
    def monitor(self) -> Tuple[pd.DataFrame, str]:
        """
        执行持仓监控
        
        Returns:
            (持仓分析DataFrame, 报告文本)
        """
        from .. import config as cfg
        from ..data import fetch_realtime_quotes
        from ..factors import get_market_breadth_pro

        if not self.engine.holdings:
            logger.warning("[持仓] 未配置持仓列表")
            return pd.DataFrame(), ""

        logger.info("=" * 60)
        logger.info("[持仓诊断] 实时持仓分析")
        logger.info("=" * 60)

        # 获取实时行情
        df_real = fetch_realtime_quotes(fs='沪深A股')
        if df_real is None or df_real.empty:
            logger.error("获取实时行情失败")
            return pd.DataFrame(), ""

        self.engine.realtime_quotes = df_real

        df_main = df_real[
            df_real['股票代码'].str.startswith(('60', '00')) &
            ~df_real['股票名称'].str.contains('ST')
        ].copy()

        df_main['成交额'] = pd.to_numeric(df_main['成交额'], errors='coerce')
        df_main['总市值'] = pd.to_numeric(df_main['总市值'], errors='coerce')

        self.engine.market_total_amount = df_main['成交额'].sum()

        # 构建分析池
        reference_codes = df_main.sort_values('成交额', ascending=False).head(100)['股票代码'].tolist()
        task_codes = list(set(self.engine.holdings + reference_codes))

        code_info = {}
        for _, row in df_main.iterrows():
            code_info[row['股票代码']] = (row['股票名称'], row['总市值'])

        # 计算因子
        results = []
        with ThreadPoolExecutor(max_workers=self.engine.max_io_workers) as executor:
            futures = {
                executor.submit(
                    self.engine.calculate_indicators_cached,
                    c,
                    code_info.get(c, (c, 0))[0],
                    code_info.get(c, (c, 0))[1],
                    skip_nlp=True
                ): c for c in task_codes
            }
            for f in tqdm(as_completed(futures), total=len(task_codes), desc="持仓分析"):
                res = f.result()
                if res:
                    results.append(res)

        if not results:
            logger.warning("[持仓] 无法计算持仓因子")
            return pd.DataFrame(), ""

        # 风格中性化
        df_all = self.engine.industry_neutralization_with_trend(pd.DataFrame(results))

        # 提取持仓
        df_hold = df_all[df_all['code'].isin(self.engine.holdings)].copy()

        if df_hold.empty:
            logger.warning("[持仓] 持仓股票不在分析结果中")
            return pd.DataFrame(), ""

        # 【修复】用实时行情价格替换K线收盘价，确保显示的是当前市价
        realtime_prices = df_real.set_index('股票代码')['最新价'].to_dict()
        df_hold['realtime_price'] = df_hold['code'].map(realtime_prices)
        # 如果有实时价格就用实时价，否则保留K线收盘价
        df_hold['close'] = df_hold['realtime_price'].fillna(df_hold['close'])

        # NLP分析 (仅持仓)
        df_hold = self._apply_nlp_analysis(df_hold, cfg)

        # 止盈止损判定
        df_hold['action'] = df_hold.apply(self.engine._logic_exit_check, axis=1)

        # 生成报告
        report_text = self.engine._display_portfolio_report(df_hold)

        return df_hold, report_text
    
    def _apply_nlp_analysis(self, df_hold: pd.DataFrame, cfg) -> pd.DataFrame:
        """对持仓股票应用NLP分析"""
        if not getattr(cfg, 'ENABLE_NLP_ANALYSIS', False):
            return df_hold
            
        logger.info(f"[NLP] 对 {len(df_hold)} 只持仓股票进行情绪分析")
        nlp_results = {}
        for _, row in df_hold.iterrows():
            code = row['code']
            try:
                from ..factors.nlp import analyze_sentiment
                score, _ = analyze_sentiment(code, self.engine.dxy_val, self.engine.dxy_trend)
                nlp_results[code] = score
            except Exception as e:
                logger.warning(f"[NLP] {code} 分析失败: {e}")
                nlp_results[code] = 0.0
        
        df_hold['nlp_score'] = df_hold['code'].map(nlp_results).fillna(0.0)
        return df_hold
