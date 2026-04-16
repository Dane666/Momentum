# -*- coding: utf-8 -*-
"""
Scanner - 市场扫描选股模块

职责:
- 全市场股票扫描
- 量化因子计算
- 风格中性化选股
- 行业分散化筛选

从 engine.py 提取，遵循单一职责原则。
"""

import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Optional, Dict, List, Tuple
import logging

logger = logging.getLogger('momentum.scanner')


class MarketScanner:
    """
    市场扫描器
    
    执行全市场选股流程:
    1. 市场情绪分析 (连板择时、互联互通)
    2. 市场宽度检查
    3. 候选筛选与因子计算
    4. NLP精选分析
    5. 行业分散化选股
    """
    
    def __init__(self, engine):
        """
        初始化扫描器
        
        Args:
            engine: MomentumEngine 实例 (用于访问共享状态和方法)
        """
        self.engine = engine
    
    def scan(self) -> Tuple[pd.DataFrame, str]:
        """
        执行市场扫描
        
        Returns:
            (选股结果DataFrame, 报告文本)
        """
        from .. import config as cfg
        from ..data import fetch_realtime_quotes, save_factor_logs
        from ..factors import (
            get_limit_up_streak,
            get_connect_sentiment,
            get_market_breadth_pro,
            get_position_multiplier,
        )
        from ..notify import send_feishu_msg

        start_t = time.time()

        # ========== Phase 1: 市场情绪分析 ==========
        logger.info("=" * 60)
        logger.info("[Phase 1] 市场情绪与资金流向分析")
        logger.info("=" * 60)

        streak, self.engine.streak_emotion = get_limit_up_streak()
        connect_score, self.engine.connect_trend = get_connect_sentiment()
        
        from ..factors.market import get_dxy_status
        self.engine.dxy_val, self.engine.dxy_trend, _ = get_dxy_status()
        logger.info(f"[宏观] 美元指数: {self.engine.dxy_val} ({self.engine.dxy_trend})")

        self.engine.position_multiplier = get_position_multiplier(
            self.engine.streak_emotion,
            self.engine.connect_trend
        )
        logger.info(f"[仓位系数] {self.engine.position_multiplier:.2f}x "
                   f"(情绪: {self.engine.streak_emotion}, 外资: {self.engine.connect_trend})")

        # ========== Phase 2: 市场宽度 ==========
        df_real = fetch_realtime_quotes(fs='沪深A股')
        if df_real is None or df_real.empty:
            logger.error("获取实时行情失败")
            return pd.DataFrame(), ""

        self.engine.realtime_quotes = df_real

        df_main = df_real[
            df_real['股票代码'].str.startswith(('60', '00')) &
            ~df_real['股票名称'].str.contains('ST')
        ].copy()

        df_main['涨跌幅'] = pd.to_numeric(df_main['涨跌幅'], errors='coerce')
        df_main['量比'] = pd.to_numeric(df_main['量比'], errors='coerce')
        df_main['成交额'] = pd.to_numeric(df_main['成交额'], errors='coerce')
        df_main['总市值'] = pd.to_numeric(df_main['总市值'], errors='coerce')

        self.engine.market_total_amount = df_main['成交额'].sum()
        self.engine.market_breadth = get_market_breadth_pro(df_main)

        logger.info(f"[市场宽度] {self.engine.market_breadth:.2%}，成交额: {self.engine.market_total_amount/1e8:.0f}亿")

        # 【已移除市场防御】允许在任何市场环境下扫描，以与Backtest逻辑保持一致
        # if self.engine.market_breadth < cfg.MARKET_BREADTH_DEFENSE:
        #     logger.warning(f"🚫 市场宽度不足，进入空仓防御")
        #     return pd.DataFrame(), ""

        # ========== Phase 3: 候选筛选与因子计算 ==========
        filter_stats = self._filter_candidates(df_main, cfg)
        candidates = filter_stats.pop('_candidates')
        vol_ratio_map = filter_stats.pop('_vol_ratio_map', {})  # 初筛阶段计算的量比
        
        # 打印筛选统计（无论候选是否为空都打印）
        print(f"\n[候选筛选] 全市场: {filter_stats.get('全市场', 0)} → "
              f"涨幅: {filter_stats.get('涨幅筛选', 0)} → "
              f"量比: {filter_stats.get('量比筛选', 0)} → "
              f"成交额: {filter_stats.get('成交额筛选', 0)}\n")
        
        if candidates.empty:
            logger.warning("🚫 候选筛选后无股票，建议检查涨幅/量比/成交额阈值")
            return pd.DataFrame(), filter_stats

        task_list = candidates.head(60)[['股票代码', '股票名称', '总市值']].values.tolist()

        quant_list = []
        with ThreadPoolExecutor(max_workers=self.engine.max_io_workers) as executor:
            futures = {
                executor.submit(self.engine.calculate_indicators_cached, s[0], s[1], s[2], skip_nlp=True): s
                for s in task_list
            }
            for f in tqdm(as_completed(futures), total=len(task_list), desc="量化因子"):
                res = f.result()
                if res:
                    quant_list.append(res)

        if not quant_list:
            return pd.DataFrame(), ""

        df_quant = self.engine.industry_neutralization_with_trend(pd.DataFrame(quant_list))
        change_pct_map = df_main.set_index('股票代码')['涨跌幅'].to_dict()
        df_quant['change_pct'] = df_quant['code'].map(change_pct_map).fillna(df_quant['change_pct'])
        
        # 用初筛计算的量比替换因子阶段的量比，保持一致性
        if vol_ratio_map:
            df_quant['vr_t'] = df_quant['code'].map(vol_ratio_map).fillna(df_quant['vr_t'])
        
        filter_stats['量化计算'] = len(df_quant)

        # ========== Phase 3.5: NLP精选 ==========
        df_quant = self._apply_nlp_analysis(df_quant, cfg)

        # ========== Phase 4: 行业分散选股 ==========
        df_result, filter_stats = self._sector_diversified_select(df_quant, cfg, filter_stats)

        if not df_result.empty:
            save_factor_logs(df_result, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        report_text = self.engine._display_report(df_result, start_t, filter_stats)

        return df_result, report_text
    
    def _filter_candidates(self, df_main: pd.DataFrame, cfg) -> Dict:
        """筛选候选股票"""
        from ..data.fetcher import batch_calculate_vol_ratio
        from tqdm import tqdm
        
        filter_stats = {'全市场': len(df_main)}
        vol_ratio_map = {}  # 保存计算的量比，用于后续阶段

        # 涨幅区间筛选
        candidates = df_main[
            (df_main['涨跌幅'] >= cfg.MIN_CHANGE_PCT) &
            (df_main['涨跌幅'] <= cfg.MAX_CHANGE_PCT)
        ].copy()
        filter_stats['涨幅筛选'] = len(candidates)

        # 【已移除量比严格筛选】计算量比用于展示，但不过滤 - 以与Backtest逻辑保持一致
        codes_to_calc = candidates['股票代码'].tolist()
        if codes_to_calc:
            print(f"[量比计算] 正在从K线计算 {len(codes_to_calc)} 只股票的量比用于展示...")
            vol_ratio_map = batch_calculate_vol_ratio(codes_to_calc, max_workers=30)
            candidates['量比'] = candidates['股票代码'].map(vol_ratio_map).fillna(1.0)
            print(f"[量比计算] 完成，量比统计范围: {candidates['量比'].min():.2f}-{candidates['量比'].max():.2f}")
        filter_stats['量比筛选'] = len(candidates)  # 不过滤，统计数与涨幅筛选相同

        # 成交额筛选
        candidates = candidates[candidates['成交额'] >= cfg.MIN_AMOUNT]
        filter_stats['成交额筛选'] = len(candidates)

        # 按涨幅排序
        candidates = candidates.sort_values('涨跌幅', ascending=False)
        
        filter_stats['_candidates'] = candidates
        filter_stats['_vol_ratio_map'] = vol_ratio_map  # 传递量比映射
        return filter_stats
    
    def _apply_nlp_analysis(self, df_quant: pd.DataFrame, cfg) -> pd.DataFrame:
        """应用NLP分析"""
        nlp_candidate_size = getattr(cfg, 'NLP_CANDIDATE_SIZE', 10)
        if not getattr(cfg, 'ENABLE_NLP_ANALYSIS', False):
            return df_quant
            
        logger.info(f"[Phase 3.5] NLP精选分析 (Top {nlp_candidate_size} 候选)")
        
        df_top_candidates = df_quant.nlargest(nlp_candidate_size, 'alpha_score')
        
        nlp_results = {}
        for _, row in tqdm(df_top_candidates.iterrows(), total=len(df_top_candidates), desc="NLP分析"):
            code = row['code']
            try:
                from ..factors.nlp import analyze_sentiment
                score, _ = analyze_sentiment(code, self.engine.dxy_val, self.engine.dxy_trend)
                nlp_results[code] = score
            except Exception as e:
                logger.warning(f"[NLP] {code} 分析失败: {e}")
                nlp_results[code] = 0.0
        
        df_quant['nlp_score'] = df_quant['code'].map(nlp_results).fillna(0.0)
        df_quant = self.engine.industry_neutralization_with_trend(df_quant)
        
        logger.info(f"[NLP] 完成 {len(nlp_results)} 只股票的情绪分析")
        return df_quant
    
    def _sector_diversified_select(
        self, 
        df_quant: pd.DataFrame, 
        cfg,
        filter_stats: Dict
    ) -> Tuple[pd.DataFrame, Dict]:
        """行业分散化选股"""
        from ..factors import get_position_multiplier
        
        # 使用仓位系数动态调整选股数量
        position_multiplier = getattr(self.engine, 'position_multiplier', 1.0)
        # 最少选2只，保证有备选，但不超过配置上限的2倍
        base_picks = max(2, cfg.MAX_TOTAL_PICKS)
        max_picks = min(base_picks * 2, int(base_picks * max(1.0, position_multiplier)))

        rsi_passed = [r for _, r in df_quant.iterrows() if r['rsi'] <= self.engine.RSI_DANGER_ZONE]
        filter_stats['RSI筛选'] = len(rsi_passed)
        sharpe_passed = [r for r in rsi_passed if r['sharpe_t'] > cfg.MIN_SHARPE]
        filter_stats['夏普筛选'] = len(sharpe_passed)

        final_picks, sector_counts = [], {}
        bias_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20)
        
        for _, row in df_quant.sort_values('alpha_score', ascending=False).iterrows():
            if len(final_picks) >= max_picks:
                break
            if row['rsi'] > self.engine.RSI_DANGER_ZONE:
                continue
            
            # 检查乖离率（提前过滤，避免占用选股名额）
            ma20 = row.get('ma20', 0)
            close = row.get('close', 0)
            if ma20 > 0:
                bias_20 = (close / ma20) - 1
                if bias_20 >= bias_limit:
                    continue
            
            # 检查是否站上MA20
            if close <= ma20:
                continue
            
            # 庄股过滤 (如果启用)
            if getattr(cfg, 'ENABLE_MANIPULATION_FILTER', False):
                manip_score = row.get('manipulation_score', 0)
                threshold = getattr(cfg, 'MANIPULATION_SCORE_THRESHOLD', 50)
                if manip_score >= threshold:
                    continue
            
            s = row['sector']
            if (sector_counts.get(s, 0) < cfg.MAX_SECTOR_PICKS and
                row['sharpe_t'] > cfg.MIN_SHARPE):
                row_dict = row.to_dict()
                row_dict['action'] = self.engine._get_action_label(row)
                final_picks.append(row_dict)
                sector_counts[s] = sector_counts.get(s, 0) + 1

        filter_stats['行业分散'] = len(final_picks)
        return pd.DataFrame(final_picks), filter_stats
