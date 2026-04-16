# -*- coding: utf-8 -*-
"""
回测模拟器
工程化版本 v2 - 同步 momentum 最新代码架构

主要优化:
1. 修复空数据导致的 KeyError
2. 支持 ATR 动态止损
3. 行业中性化选股
4. 与实盘选股逻辑保持一致
5. 增加年化收益、交易次数等指标
6. 支持交易记录持久化 (SQLite)，用于可视化回测
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Union
import logging

logger = logging.getLogger('momentum')


class MomentumBacktester:
    """
    Momentum 回测引擎
    
    与实盘选股 (engine.py) 保持一致的因子计算和选股逻辑
    
    v4 优化 - 真正的14:45分钟级数据:
    - 使用5分钟K线获取14:45时刻的真实价格
    - 使用截止14:45的累计成交额作为选股依据
    - 买入价 = 14:45真实价格 (而非日K收盘价)
    
    回测逻辑说明:
    - 实盘在14:45进行scan选股，使用当时的价格和累计成交额
    - 回测使用5分钟K线的14:45数据，完全复现实盘逻辑
    """

    def __init__(
        self, 
        backtest_days: Optional[int] = None, 
        hold_period: Optional[int] = None, 
        record_trades: bool = True,
        window_shift: int = 0,
        use_1445_data: bool = True,  # 是否使用14:45真实数据
    ):
        """
        初始化回测器

        Args:
            backtest_days: 回测天数
            hold_period: 持仓周期
            record_trades: 是否记录交易到数据库 (用于可视化)
            window_shift: 窗口偏移天数 (用于滑动窗口稳定性测试)
            use_1445_data: 是否使用14:45真实分钟数据 (默认True)
        """
        from .. import config as cfg
        from ..data import init_db
        from ..factors import get_style_group

        self.backtest_days = backtest_days or cfg.BACKTEST_DAYS_DEFAULT
        self.hold_period = hold_period or cfg.HOLD_PERIOD_DEFAULT
        self.pool_size = cfg.POOL_SIZE
        self.record_trades = record_trades
        self._window_shift = window_shift  # 滑动窗口偏移
        self._use_1445_data = use_1445_data  # 使用14:45真实数据

        # 从 config 读取参数 (与实盘保持一致)
        self.slippage = cfg.SLIPPAGE
        self.atr_stop_factor = cfg.ATR_STOP_FACTOR
        self.min_amount = cfg.MIN_AMOUNT
        self.min_sharpe = cfg.MIN_SHARPE
        self.max_sector_picks = cfg.MAX_SECTOR_PICKS
        self.max_total_picks = cfg.MAX_TOTAL_PICKS
        self.nlp_score_default = cfg.NLP_SCORE_DEFAULT
        self.market_amount_high = cfg.MARKET_AMOUNT_HIGH
        self.rsi_danger_zone = getattr(cfg, 'RSI_DANGER_ZONE', 85.0)
        
        # 庄股识别配置
        self.enable_manipulation_filter = getattr(cfg, 'ENABLE_MANIPULATION_FILTER', False)
        self.manipulation_score_threshold = getattr(cfg, 'MANIPULATION_SCORE_THRESHOLD', 50)
        self.momentum_r2_threshold = getattr(cfg, 'MOMENTUM_R2_THRESHOLD', 0.90)
        self.ivol_percentile = getattr(cfg, 'IVOL_PERCENTILE', 0.95)
        self.illiq_percentile = getattr(cfg, 'ILLIQ_PERCENTILE', 0.95)
        self.overnight_ratio_threshold = getattr(cfg, 'OVERNIGHT_RATIO_THRESHOLD', 0.75)

        # 缓存
        self.all_data_cache = {}
        self.stock_info_cache = {}
        self.sector_cache = {}
        self._5min_data_cache = {}  # 5分钟K线缓存 (14:45数据)
        self._1445_data_cache = {}  # 14:45时刻数据缓存 {(code, date): dict}

        # 工具函数
        self._get_style_group = get_style_group

        # 初始化数据库
        init_db()
        
        # 交易记录器 (延迟初始化)
        self.trade_recorder = None
        
        # 账户资金设置 (默认10万)
        self.initial_capital = getattr(cfg, 'INITIAL_CAPITAL', 100000.0)

        shift_info = f", shift={window_shift}" if window_shift > 0 else ""
        mode_info = "14:45分钟级真实数据" if use_1445_data else "日K收盘价近似"
        logger.info(f"[Backtest] 初始化: days={self.backtest_days}, hold={self.hold_period}, record={record_trades}, capital={self.initial_capital:.0f}{shift_info}")
        logger.info(f"[Backtest] 数据模式: {mode_info}")

    def _get_action_label(self, row) -> str:
        """
        获取操作标签 (与实盘 engine.py 保持一致)
        
        Args:
            row: 包含 alpha_score, alpha_trend 等字段的 Series
            
        Returns:
            操作标签字符串
        """
        alpha_score = row.get('alpha_score', 0)
        alpha_trend = row.get('alpha_trend', 0)
        
        # 所有通过选股过滤的股票都符合回测买入标准
        if alpha_score > 1.2 and alpha_trend > 0:
            return "🎯 [强买入]"  # 完全符合回测条件的强信号
        elif alpha_score > 0.8 and alpha_trend > 0.3:
            return "✅ [次强买入]"  # 符合回测条件的次强信号
        else:
            return "📊 [回测买入]"  # 符合回测条件的基础信号

    def _batch_preload_sectors(self, codes: List[str]):
        """
        批量预加载板块信息到SQLite缓存
        
        性能优化关键:
        - 先检查哪些股票的板块信息已缓存
        - 只对未缓存的股票批量获取板块信息
        - 避免在并发加载K线时触发3000+次网络请求
        
        Args:
            codes: 股票代码列表
        """
        from ..data import get_db_connection, fetch_stock_concept
        from datetime import datetime, timedelta
        
        try:
            # 1. 检查已缓存的股票
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT code FROM stock_sector_cache WHERE update_time > ?",
                             ((datetime.now() - timedelta(days=7)).isoformat(),))
                cached_codes = set(row[0] for row in cursor.fetchall())
            
            # 2. 找出未缓存的股票
            uncached_codes = [c for c in codes if c not in cached_codes]
            
            if not uncached_codes:
                logger.info(f"[Backtest] 板块信息全部命中缓存 ({len(cached_codes)} 只)")
                return
            
            logger.info(f"[Backtest] 预加载板块信息: 已缓存 {len(cached_codes)} 只, 待获取 {len(uncached_codes)} 只")
            
            # 3. 批量获取未缓存的板块信息 (带进度条)
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            def fetch_sector_worker(code):
                sector = fetch_stock_concept(code, use_cache=True)  # 会自动写入缓存
                return code, sector
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(fetch_sector_worker, c) for c in uncached_codes]
                for f in tqdm(as_completed(futures), total=len(uncached_codes), desc="加载板块", leave=False):
                    code, sector = f.result()
                    self.sector_cache[code] = sector
            
            logger.info(f"[Backtest] 板块信息预加载完成")
            
        except Exception as e:
            logger.warning(f"[Backtest] 板块信息批量加载失败: {e}, 将使用逐个获取模式")

    def prepare_backtest_data(self) -> List[tuple]:
        """
        预载入全市场主板股票K线数据
        
        策略：
        1. 加载全市场主板股票的K线数据（不预先筛选）
        2. 在每个回测日，根据当天的成交额排名动态选择候选股
        3. 这样完全模拟实盘scan逻辑，且结果可复现
        """
        from ..data import fetch_realtime_quotes, fetch_kline_from_api, fetch_stock_concept, fetch_all_stock_codes
        from .. import config as cfg

        logger.info(f"正在初始化 {self.backtest_days}日 回测底池 (全市场模式)...")

        # 获取全市场股票代码
        codes = fetch_all_stock_codes()
        if not codes:
            df_real = fetch_realtime_quotes(fs='沪深A股')
            if df_real is not None and not df_real.empty:
                codes = df_real['股票代码'].tolist()
            else:
                logger.error("获取股票代码失败")
                return []
        
        # 筛选主板股票（沪市60开头，深市00开头）
        main_codes = sorted([c for c in codes if c.startswith(('60', '00'))])
        
        # 获取股票名称（用于过滤ST）
        df_real = fetch_realtime_quotes(fs='沪深A股')
        st_codes = set()
        code_names = {}
        if df_real is not None and not df_real.empty:
            for _, row in df_real.iterrows():
                code = row['股票代码']
                name = row.get('股票名称', '')
                code_names[code] = name
                if 'ST' in str(name):
                    st_codes.add(code)
        
        # 过滤ST股票
        main_codes = [c for c in main_codes if c not in st_codes]
        logger.info(f"[Backtest] 主板非ST股票数: {len(main_codes)}")
        
        # 保存股票名称映射
        self.code_names = code_names

        # 【性能优化】批量预加载板块信息到缓存
        # 这样后续并发加载K线时，可以直接从SQLite缓存读取，避免3000+次网络请求
        self._batch_preload_sectors(main_codes)

        def fetch_worker(code):
            """并发获取 K 线数据"""
            try:
                from ..data.cache import load_or_fetch_kline
                df = load_or_fetch_kline(code, fetch_kline_from_api, cfg.KLINE_START_DATE)
                if df is not None and len(df) > 100:
                    # 标准化字段
                    df['close'] = pd.to_numeric(df['close'], errors='coerce')
                    df['high'] = pd.to_numeric(df['high'], errors='coerce')
                    df['low'] = pd.to_numeric(df['low'], errors='coerce')
                    df['open'] = pd.to_numeric(df.get('open', df['close']), errors='coerce')
                    df['vol'] = pd.to_numeric(df['volume'], errors='coerce')
                    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                    df['turnover_rate'] = pd.to_numeric(
                        df.get('turnover_ratio', df.get('turnover_rate', 0)),
                        errors='coerce'
                    ).fillna(0)
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()

                    # 缓存板块信息
                    if code not in self.sector_cache:
                        self.sector_cache[code] = fetch_stock_concept(code)

                    return code, df.sort_values('trade_date').reset_index(drop=True)
            except Exception as e:
                logger.debug(f"[Backtest] {code} 数据加载失败: {e}")
            return None, None

        # 并发获取全市场K线数据
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(fetch_worker, c) for c in main_codes]
            for f in tqdm(as_completed(futures), total=len(main_codes), desc="加载K线"):
                code, df = f.result()
                if df is not None:
                    self.all_data_cache[code] = df
                    self.stock_info_cache[code] = 10e9  # 默认市值

        logger.info(f"[Backtest] 已缓存 {len(self.all_data_cache)} 只股票K线")
        
        # 预加载14:45数据到内存 (如果启用)
        if self._use_1445_data:
            self._preload_1445_data()
        
        # 返回全部股票池（实际选股在每个回测日动态进行）
        pool = [(code, code_names.get(code, code), 10e9) for code in self.all_data_cache.keys()]
        return pool

    def _preload_1445_data(self):
        """
        预加载所有回测日期的14:45数据到内存
        
        优化策略:
        1. 批量从SQLite缓存加载已有数据
        2. 对于缺失的数据，不在此处获取（延迟到实际使用时）
        3. 避免阻塞，让回测快速启动
        """
        from ..data import load_1445_data_batch
        
        # 获取回测日期范围
        all_dates = set()
        for code, df in self.all_data_cache.items():
            if df is not None and 'trade_date' in df.columns:
                dates = df['trade_date'].dt.strftime('%Y-%m-%d').tolist()
                # 只取最近 backtest_days + hold_period 天
                recent_dates = dates[-(self.backtest_days + self.hold_period + 5):]
                all_dates.update(recent_dates)
        
        if not all_dates:
            logger.warning("[Backtest] 无法确定回测日期范围")
            return
        
        dates_list = sorted(list(all_dates))
        codes_list = list(self.all_data_cache.keys())
        
        logger.info(f"[Backtest] 预加载14:45数据: {len(codes_list)} 只股票 × {len(dates_list)} 天")
        
        # 批量加载已缓存的14:45数据
        cached_data = load_1445_data_batch(codes_list, dates_list)
        
        # 写入内存缓存
        for (code, date_str), data in cached_data.items():
            if data and data.get('price_1445', 0) > 0:
                self._1445_data_cache[(code, date_str)] = data
        
        cache_hit = len(self._1445_data_cache)
        cache_miss = len(codes_list) * len(dates_list) - cache_hit
        
        logger.info(f"[Backtest] 14:45缓存命中: {cache_hit} 条, 待获取: {cache_miss} 条")

    def _get_daily_top_stocks(self, t_date, top_n: int = 150) -> List[str]:
        """
        获取指定日期成交额排名前N的股票
        
        v5优化 - 纯缓存模式:
        - 只使用内存缓存或日K数据
        - 不触发网络请求，避免进度条卡顿
        - 14:45数据缺失时回退到日K成交额
        
        Args:
            t_date: 回测日期 (T日，即选股当天)
            top_n: 取前N只
            
        Returns:
            股票代码列表
        """
        target_t = pd.to_datetime(t_date).normalize()
        target_date_str = target_t.strftime('%Y-%m-%d')
        
        # 收集所有股票当天的成交额
        daily_amounts = []
        for code, df in self.all_data_cache.items():
            # 找到目标日期的数据
            day_data = df[df['trade_date'] == target_t]
            if not day_data.empty:
                amount = day_data['amount'].iloc[0]  # 默认使用日K成交额
                
                if self._use_1445_data:
                    # 只检查内存缓存，不触发网络请求
                    cache_key = (code, target_date_str)
                    if cache_key in self._1445_data_cache:
                        data_1445 = self._1445_data_cache[cache_key]
                        if data_1445 and data_1445.get('amount_1445', 0) > 0:
                            amount = data_1445['amount_1445']
                
                if pd.notna(amount) and amount > 0:
                    daily_amounts.append((code, amount))
        
        # 按成交额排序
        daily_amounts.sort(key=lambda x: x[1], reverse=True)
        
        # 返回前N只
        top_codes = [x[0] for x in daily_amounts[:top_n]]
        return top_codes
    
    def _get_1445_data(self, code: str, target_date: str) -> Optional[dict]:
        """
        获取指定股票在指定日期14:45时刻的数据
        
        优先级:
        1. 内存缓存 (_1445_data_cache)
        2. SQLite轻量级缓存 (data_1445_cache 表)
        3. 5分钟K线提取 (并写入缓存)
        
        Args:
            code: 股票代码
            target_date: 目标日期 (YYYY-MM-DD)
            
        Returns:
            dict with 14:45 data or None
        """
        cache_key = (code, target_date)
        
        # 1. 检查内存缓存
        if cache_key in self._1445_data_cache:
            return self._1445_data_cache[cache_key]
        
        # 2. 检查SQLite轻量级缓存
        from ..data import load_1445_data, save_1445_data
        
        cached = load_1445_data(code, target_date)
        if cached and cached.get('price_1445', 0) > 0:
            self._1445_data_cache[cache_key] = cached
            return cached
        
        # 3. 从5分钟K线提取
        from ..data import fetch_5min_kline, extract_1445_data
        
        if code not in self._5min_data_cache:
            df_5min = fetch_5min_kline(code)
            if df_5min is not None and not df_5min.empty:
                self._5min_data_cache[code] = df_5min
            else:
                self._5min_data_cache[code] = None
        
        df_5min = self._5min_data_cache.get(code)
        if df_5min is None:
            self._1445_data_cache[cache_key] = None
            return None
        
        # 提取14:45数据
        data_1445 = extract_1445_data(df_5min, target_date)
        self._1445_data_cache[cache_key] = data_1445
        
        # 4. 写入SQLite缓存 (供下次快速读取)
        if data_1445:
            save_1445_data(code, target_date, data_1445)
        
        return data_1445

    def _calculate_rsi(self, closes: pd.Series, period: int = 14) -> float:
        """计算 RSI 指标"""
        if len(closes) < period + 1:
            return 50.0
        
        delta = closes.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        
        avg_gain = gain.rolling(period).mean().iloc[-1]
        avg_loss = loss.rolling(period).mean().iloc[-1]
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _calc_manipulation_factors(
        self, df: pd.DataFrame, window: int = 20
    ) -> tuple:
        """
        计算庄股识别因子
        
        Returns:
            (manipulation_score, momentum_r2, ivol, illiq, overnight_ratio)
        """
        from scipy import stats
        
        try:
            # 1. Momentum Quality (R²)
            log_close = np.log(df['close'].tail(window).values)
            x = np.arange(len(log_close))
            _, _, r_value, _, _ = stats.linregress(x, log_close)
            momentum_r2 = r_value ** 2
            
            # 2. IVOL (特质波动率)
            ret = df['close'].pct_change().tail(window).dropna()
            # 使用简化版本: 直接计算残差波动率 (假设市场收益率=0)
            ivol = ret.std() if len(ret) > 5 else 0.0
            
            # 3. Amihud Illiquidity
            abs_ret = ret.abs()
            turnover = (df['close'] * df['vol']).tail(window)
            turnover = turnover.replace(0, np.nan)
            illiq = (abs_ret / turnover.iloc[1:].values).mean() * 1e6 if len(turnover) > 1 else 0.0
            if np.isnan(illiq) or np.isinf(illiq):
                illiq = 0.0
            
            # 4. Overnight vs Intraday
            overnight_ret = np.log(df['open'] / df['close'].shift(1)).tail(window).sum()
            intraday_ret = np.log(df['close'] / df['open']).tail(window).sum()
            total = abs(overnight_ret) + abs(intraday_ret)
            overnight_ratio = abs(overnight_ret) / total if total > 0 else 0.0
            
            # 5. 综合评分 (0-100)
            score = 0.0
            if momentum_r2 > self.momentum_r2_threshold:
                score += 25
            # IVOL和ILLIQ使用相对阈值，这里使用简化规则
            if ivol > 0.05:  # 日波动率>5%
                score += 25
            if illiq > 0.1:  # 非流动性较高
                score += 25
            if overnight_ratio > self.overnight_ratio_threshold:
                score += 25
            
            return score, momentum_r2, ivol, illiq, overnight_ratio
            
        except Exception as e:
            logger.debug(f"[Manipulation] 因子计算失败: {e}")
            return 0.0, 0.0, 0.0, 0.0, 0.0

    def _simulate_smart_exit(
        self, code: str, t_idx: int, full_df: pd.DataFrame, atr: float, 
        entry_price_1445: Optional[float] = None
    ) -> tuple:
        """
        模拟智能出场逻辑 (与持仓监控/选股统一)
        
        v4版本 - 使用14:45真实价格:
        - 买入价 = 14:45真实价格 (如果有5分钟K线数据)
        - 回退: 使用当日收盘价
        
        委托给 risk 模块统一实现。
        
        Args:
            code: 股票代码
            t_idx: 选股日索引 (T日，即买入日)
            full_df: 完整K线数据
            atr: ATR值
            entry_price_1445: 14:45真实价格 (可选)
        
        Returns:
            (fwd_ret, exit_reason, actual_hold_days, exit_date)
        """
        from ..risk import ExitRuleEngine
        
        # 优先使用14:45真实价格，否则使用收盘价
        if entry_price_1445 is not None and entry_price_1445 > 0:
            entry_price = entry_price_1445
        else:
            entry_price = full_df['close'].iloc[t_idx]
        
        # 使用自适应止损模式 (可通过配置开关)
        from .. import config as cfg
        use_adaptive = getattr(cfg, 'USE_ADAPTIVE_EXIT', False)
        
        engine = ExitRuleEngine(adaptive=use_adaptive)
        return engine.simulate_exit(
            entry_price=entry_price,
            df=full_df,
            entry_idx=t_idx,
            hold_period=self.hold_period,
            slippage=self.slippage
        )

    def _simulate_day_data(self, t_date, code: str, name: str, mkt_cap: float) -> Optional[dict]:
        """
        在历史时间点执行因子计算
        
        v5优化 - 纯缓存模式:
        - 只使用内存缓存中的14:45数据
        - 不触发网络请求，避免进度条卡顿
        - 14:45数据缺失时回退到日K收盘价
        
        与实盘 calculate_indicators_cached 保持一致的因子计算逻辑
        
        Args:
            t_date: 目标日期 (T日，即选股+买入日)
            code: 股票代码
            name: 股票名称
            mkt_cap: 市值
        """
        try:
            full_df = self.all_data_cache.get(code)
            if full_df is None:
                return None
                
            target_t = pd.to_datetime(t_date).normalize()
            target_date_str = target_t.strftime('%Y-%m-%d')
            snap_df = full_df[full_df['trade_date'] <= target_t].copy()

            # 数据充足性检查
            if len(snap_df) < 35 or snap_df['trade_date'].iloc[-1] != target_t:
                return None

            # 获取14:45真实数据 (只检查内存缓存，不触发网络请求)
            data_1445 = None
            use_1445 = False
            if self._use_1445_data:
                cache_key = (code, target_date_str)
                if cache_key in self._1445_data_cache:
                    data_1445 = self._1445_data_cache[cache_key]
                    use_1445 = data_1445 is not None and data_1445.get('price_1445', 0) > 0
            
            # 使用14:45价格或日K收盘价
            if use_1445:
                current_price = data_1445['price_1445']
                current_amount = data_1445.get('amount_1445', snap_df['amount'].iloc[-1])
            else:
                current_price = snap_df['close'].iloc[-1]
                current_amount = snap_df['amount'].iloc[-1]

            # 成交额过滤
            if current_amount < self.min_amount:
                return None

            # 计算收益率 (使用日K数据，因为需要历史序列)
            snap_df['ret'] = snap_df['close'].pct_change()

            def compute_metrics(df_slice, use_1445_price: bool = False, price_1445: Optional[float] = None):
                """计算单日因子"""
                if len(df_slice) < 20:
                    return None
                
                # 如果使用14:45价格，替换最后一天的收盘价
                if use_1445_price and price_1445 is not None and price_1445 > 0:
                    close_series = df_slice['close'].copy()
                    close_series.iloc[-1] = price_1445
                else:
                    close_series = df_slice['close']
                    
                v20 = df_slice['ret'].tail(20).std() + 1e-9
                mom_5 = (close_series.iloc[-1] / close_series.iloc[-5]) - 1
                mom_20 = ((close_series.iloc[-1] / close_series.iloc[-20]) - 1) * (0.02 / v20)
                
                curr_to = df_slice['turnover_rate'].iloc[-1]
                to_mult = 1.15 if 12 < curr_to < 18 else (0.6 if curr_to < 3 and df_slice['ret'].iloc[-1] > 0.05 else 1.0)
                
                sharpe = (df_slice['ret'].tail(20).mean() / v20) * np.sqrt(252)
                
                tr = (df_slice['high'] - df_slice['low']).tail(20)
                atr = tr.mean()
                
                vr = df_slice['vol'].iloc[-1] / (df_slice['vol'].tail(6).iloc[:-1].mean() + 1e-9)
                
                return mom_5, mom_20 * to_mult, sharpe, vr, curr_to, atr

            # 使用14:45价格计算因子 (如果有)
            use_1445 = data_1445 is not None and data_1445.get('price_1445', 0) > 0
            price_1445_val: Optional[float] = data_1445.get('price_1445') if data_1445 else None
            
            metrics_t = compute_metrics(snap_df, use_1445, price_1445_val)
            metrics_y = compute_metrics(snap_df.iloc[:-1])  # T-1日 (不使用14:45数据)
            
            if metrics_t is None or metrics_y is None:
                return None

            mom5_t, mom20_t, sh_t, vr_t, to_t, atr_t = metrics_t
            mom5_y, mom20_y, sh_y, vr_y, to_y, atr_y = metrics_y

            # MA 计算 (使用14:45价格)
            ma5_values = snap_df['close'].rolling(5).mean()
            ma20_values = snap_df['close'].rolling(20).mean()
            ma5_curr = ma5_values.iloc[-1]
            ma20_curr = ma20_values.iloc[-1]
            
            # 使用14:45价格计算bias
            close_price = current_price
            bias_20 = (close_price / ma20_curr) - 1 if ma20_curr > 0 else 0

            # RSI 计算 (使用日K数据)
            rsi = self._calculate_rsi(snap_df['close'])

            # 模拟前向收益
            t_idx = full_df[full_df['trade_date'] == target_t].index[0]
            
            # 使用14:45价格作为买入价
            buy_price = current_price
            fwd_ret, reason, actual_hold_days, exit_date = self._simulate_smart_exit(
                code, t_idx, full_df, atr_t, entry_price_1445=buy_price
            )

            # 计算庄股识别因子 (仅当启用时)
            manipulation_score = 0.0
            momentum_r2 = 0.0
            ivol = 0.0
            illiq = 0.0
            overnight_ratio = 0.0
            
            if self.enable_manipulation_filter and len(snap_df) >= 20:
                manipulation_score, momentum_r2, ivol, illiq, overnight_ratio = \
                    self._calc_manipulation_factors(snap_df)

            return {
                'code': code, 
                'name': name,
                'sector': self.sector_cache.get(code, "其它"),
                'style_group': self._get_style_group(mkt_cap),
                # T日因子 (使用14:45真实数据)
                'mom_5_t': mom5_t, 
                'mom_20_t': mom20_t, 
                'sharpe_t': sh_t,
                'vr_t': vr_t, 
                'turnover_t': to_t,
                # T-1日因子 (用于计算趋势)
                'mom_5_y': mom5_y, 
                'mom_20_y': mom20_y, 
                'sharpe_y': sh_y, 
                'vr_y': vr_y,
                # 技术指标 (使用14:45价格)
                'bias_20': bias_20, 
                'rsi': rsi,
                'atr': atr_t,
                'alpha_trend': 0.0,  # 将在 _industry_neutralization 中重新计算
                # 默认值 (回测不调用 NLP 和筹码分析)
                'nlp_score': self.nlp_score_default, 
                'hk_bonus': 0.0, 
                'chip_rate': 0.0,
                'big_order_t': 0.0, 
                'big_order_y': 0.0,
                # 前向收益与出场信息
                'fwd_ret': fwd_ret, 
                'exit_reason': reason,
                'actual_hold_days': actual_hold_days,
                'exit_date': exit_date,
                # 价格数据 (使用14:45真实价格)
                'close': close_price,  # 14:45真实价格 (如有) 或日K收盘价
                'buy_price': buy_price,  # 买入价 = 14:45价格
                'ma5': ma5_curr,
                'ma20': ma20_curr,
                'change_pct': ((close_price / snap_df['close'].iloc[-2]) - 1) * 100 if len(snap_df) > 1 else 0,
                # 标记是否使用了14:45真实数据
                '_use_1445': use_1445,
                # 庄股识别因子
                'manipulation_score': manipulation_score,
                'momentum_r2': momentum_r2,
                'ivol': ivol,
                'illiq': illiq,
                'overnight_ratio': overnight_ratio,
            }
        except Exception as e:
            logger.debug(f"[Backtest] {code} 模拟失败: {e}")
            return None

    def _industry_neutralization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        风格中性化 (与实盘保持一致)
        
        委托给 alpha 模块统一实现。
        """
        from ..alpha import AlphaModel
        
        if df.empty:
            return df
        
        # 回测使用默认市场成交量 (常规市权重)
        model = AlphaModel(
            market_total_amount=1.0e12,  # 常规市
            vol_surge_limit=3.0
        )
        return model.neutralize_and_score(df)

    def _run_backtest_with_cache(self) -> Optional[Dict]:
        """
        使用已加载的缓存执行回测 (参数优化专用)
        
        跳过数据加载步骤，直接使用 all_data_cache 等缓存数据。
        """
        from ..data import fetch_market_index
        
        if not self.all_data_cache:
            logger.error("缓存为空，请先调用 prepare_backtest_data()")
            return None
        
        # 获取基准指数
        bench = fetch_market_index(index_code='000300', k_type=1)
        if bench is None or bench.empty:
            return None
        
        bench['trade_date'] = pd.to_datetime(bench['trade_date']).dt.normalize()
        all_dates = sorted(bench['trade_date'].tolist())
        
        if len(all_dates) < self.backtest_days + self.hold_period:
            return None
        
        test_dates = all_dates[-(self.backtest_days + self.hold_period):-self.hold_period]
        
        # 简化回测流程 (不记录交易)
        equity_curve = [1.0]
        daily_stats = []
        trade_count = 0
        win_count = 0
        
        rebalance_dates = test_dates[::self.hold_period]
        
        for t_date in rebalance_dates:
            top_codes = self._get_daily_top_stocks(t_date, top_n=self.pool_size)
            
            if not top_codes:
                equity_curve.append(equity_curve[-1])
                daily_stats.append({'date': t_date, 'ret': 0.0, 'picks': 0})
                continue
            
            day_results = []
            for code in top_codes:
                name = getattr(self, 'code_names', {}).get(code, code)
                mkt_cap = self.stock_info_cache.get(code, 10e9)
                res = self._simulate_day_data(t_date, code, name, mkt_cap)
                if res:
                    day_results.append(res)
            
            if not day_results:
                equity_curve.append(equity_curve[-1])
                daily_stats.append({'date': t_date, 'ret': 0.0, 'picks': 0})
                continue

            # 行业中性化 (与 run_backtest 保持一致)
            df_day = self._industry_neutralization(pd.DataFrame(day_results))
            df_sorted = df_day.sort_values('alpha_score', ascending=False)
            picks, sector_counts = [], {}
            
            for _, row in df_sorted.iterrows():
                if len(picks) >= self.max_total_picks:
                    break
                if row['rsi'] > self.rsi_danger_zone:
                    continue
                if row['sharpe_t'] <= self.min_sharpe:
                    continue
                
                # 庄股过滤 (如果启用)
                if self.enable_manipulation_filter:
                    manip_score = row.get('manipulation_score', 0)
                    if manip_score >= self.manipulation_score_threshold:
                        continue
                    
                s = row['sector']
                if sector_counts.get(s, 0) < self.max_sector_picks:
                    picks.append(row)
                    sector_counts[s] = sector_counts.get(s, 0) + 1
            
            if picks:
                p_df = pd.DataFrame(picks)
                period_ret = p_df['fwd_ret'].mean()
                day_wins = len(p_df[p_df['fwd_ret'] > 0])
                trade_count += len(p_df)
                win_count += day_wins
                equity_curve.append(equity_curve[-1] * (1 + period_ret))
                daily_stats.append({'date': t_date, 'ret': period_ret, 'picks': len(picks)})
            else:
                equity_curve.append(equity_curve[-1])
                daily_stats.append({'date': t_date, 'ret': 0.0, 'picks': 0})
        
        return self._compute_metrics(equity_curve, daily_stats, trade_count, win_count)
    
    def _compute_metrics(self, curve: list, logs: list, trade_count: int, win_count: int) -> Dict:
        """计算回测指标 (不打印日志)"""
        if not logs or len(curve) < 2:
            return {'profit_pct': 0, 'sharpe': 0, 'max_dd': 0, 'win_rate': 0, 'trade_count': 0}
        
        df_log = pd.DataFrame(logs)
        total_ret = (curve[-1] - 1) * 100
        max_vals = np.maximum.accumulate(curve)
        mdd = np.max((max_vals - curve) / (max_vals + 1e-9)) * 100
        
        ret_std = df_log['ret'].std()
        ret_mean = df_log['ret'].mean()
        periods_per_year = 252 / self.hold_period
        sharpe = (ret_mean / (ret_std + 1e-9)) * np.sqrt(periods_per_year) if ret_std > 0 else 0.0
        
        win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0
        actual_trading_days = self.backtest_days
        annual_ret = (curve[-1] ** (252 / actual_trading_days) - 1) * 100 if actual_trading_days > 0 else 0
        
        return {
            'hold_period': self.hold_period,
            'profit_pct': total_ret,
            'annual_ret': annual_ret,
            'sharpe': sharpe,
            'win_rate': win_rate,
            'max_dd': mdd,
            'final_nav': curve[-1],
            'trade_count': trade_count,
        }

    def run_backtest(self) -> Optional[Dict]:
        """执行回测"""
        from ..data import fetch_market_index, BacktestTradeRecorder

        pool = self.prepare_backtest_data()
        if not pool:
            logger.error("回测底池为空")
            return None

        # 获取基准指数 (沪深300)
        bench = fetch_market_index(index_code='000300', k_type=1)
        if bench is None or bench.empty:
            logger.error("获取基准指数失败")
            return None

        bench['trade_date'] = pd.to_datetime(bench['trade_date']).dt.normalize()
        all_dates = sorted(bench['trade_date'].tolist())
        
        # 确保有足够的日期
        if len(all_dates) < self.backtest_days + self.hold_period + self._window_shift:
            logger.error(f"基准指数数据不足: {len(all_dates)} 天")
            return None
        
        # ⚠️ 滑动窗口支持: 通过 _window_shift 偏移回测窗口
        # 这允许测试策略在不同起始日期的稳定性
        if self._window_shift > 0:
            # 向前偏移窗口 (回测更早的历史区间)
            end_offset = self.hold_period + self._window_shift
            start_offset = self.backtest_days + self.hold_period + self._window_shift
            test_dates = all_dates[-start_offset:-end_offset]
            logger.info(f"[滑动窗口] 偏移 {self._window_shift} 天, 回测区间: {test_dates[0].strftime('%Y-%m-%d')} ~ {test_dates[-1].strftime('%Y-%m-%d')}")
        else:
            test_dates = all_dates[-(self.backtest_days + self.hold_period):-self.hold_period]
        
        # 创建基准净值映射
        bench_nav_map = {}
        if 'close' in bench.columns:
            bench_start_close = bench[bench['trade_date'] == test_dates[0]]['close'].values
            if len(bench_start_close) > 0:
                bench_start = bench_start_close[0]
                for _, row in bench.iterrows():
                    bench_nav_map[row['trade_date']] = row['close'] / bench_start

        equity_curve = [1.0]
        daily_stats = []
        trade_count = 0
        win_count = 0
        
        # 初始化交易记录器 (使用初始资金)
        if self.record_trades:
            self.trade_recorder = BacktestTradeRecorder(
                backtest_days=self.backtest_days,
                hold_period=self.hold_period,
                pool_size=self.pool_size,
                slippage=self.slippage,
                initial_capital=self.initial_capital
            )

        shift_info = f" | 窗口偏移: {self._window_shift}天" if self._window_shift > 0 else ""
        logger.info(f"启动 {self.backtest_days}日 回测 | 持仓: {self.hold_period}天 | 滑点: {self.slippage*100}%{shift_info}")

        # 修复: 按 hold_period 间隔换仓，而不是每天选股
        # 这样可以正确模拟"买入持有N天后卖出"的策略
        rebalance_dates = test_dates[::self.hold_period]  # 每 N 天换仓一次
        
        logger.info(f"换仓次数: {len(rebalance_dates)}, 每次最多持有 {self.max_total_picks} 只")

        for i, t_date in enumerate(tqdm(rebalance_dates, desc=f"Hold={self.hold_period}")):
            # 【关键】获取当天成交额排名前N的股票作为候选池
            top_codes = self._get_daily_top_stocks(t_date, top_n=self.pool_size)
            
            if not top_codes:
                # 当天无数据，净值不变
                equity_curve.append(equity_curve[-1])
                daily_stats.append({'date': t_date, 'ret': 0.0, 'win_rate': 0.0, 'picks': 0})
                continue
            
            # 对当天的候选股计算因子
            day_results = []
            for code in top_codes:
                name = getattr(self, 'code_names', {}).get(code, code)
                mkt_cap = self.stock_info_cache.get(code, 10e9)
                res = self._simulate_day_data(t_date, code, name, mkt_cap)
                if res:
                    day_results.append(res)

            if not day_results:
                # 本期无选股，净值不变
                for _ in range(min(self.hold_period, len(test_dates) - i * self.hold_period)):
                    equity_curve.append(equity_curve[-1])
                    daily_stats.append({'date': t_date, 'ret': 0.0, 'win_rate': 0.0, 'picks': 0})
                continue

            # 行业中性化
            df_day = self._industry_neutralization(pd.DataFrame(day_results))

            # 选股逻辑 (与实盘一致)
            df_sorted = df_day.sort_values('alpha_score', ascending=False)
            picks, sector_counts = [], {}

            for _, row in df_sorted.iterrows():
                if len(picks) >= self.max_total_picks:
                    break
                    
                # 过滤条件 (与实盘一致)
                if row['rsi'] > self.rsi_danger_zone:  # RSI > 85 则过滤（极度超买）
                    continue
                if row['sharpe_t'] <= self.min_sharpe:
                    continue
                
                # 庄股过滤 (如果启用)
                if self.enable_manipulation_filter:
                    manip_score = row.get('manipulation_score', 0)
                    if manip_score >= self.manipulation_score_threshold:
                        continue
                    
                s = row['sector']
                if sector_counts.get(s, 0) < self.max_sector_picks:
                    picks.append(row)
                    sector_counts[s] = sector_counts.get(s, 0) + 1

            if picks:
                p_df = pd.DataFrame(picks)
                # 修复: fwd_ret 是持仓期总收益，不需要除以 hold_period
                period_ret = p_df['fwd_ret'].mean()
                day_wins = len(p_df[p_df['fwd_ret'] > 0])
                daily_win = day_wins / len(p_df)
                trade_count += len(p_df)
                win_count += day_wins
                
                # 记录交易到数据库
                if self.record_trades and self.trade_recorder:
                    t_date_str = t_date.strftime('%Y-%m-%d') if hasattr(t_date, 'strftime') else str(t_date)[:10]
                    
                    # 计算每只股票的仓位金额 (等权分配)
                    num_picks = len(p_df)
                    # 使用当前账户余额计算可用资金
                    available_capital = self.trade_recorder.account_balance
                    position_value_per_stock = available_capital / num_picks if num_picks > 0 else 0
                    
                    for _, pick in p_df.iterrows():
                        # 使用实际退出日期 (从 _simulate_smart_exit 返回)
                        exit_date = pick.get('exit_date')
                        actual_hold_days = pick.get('actual_hold_days', self.hold_period)
                        
                        if hasattr(exit_date, 'strftime'):
                            sell_date_str = exit_date.strftime('%Y-%m-%d')
                        elif pd.notna(exit_date) and str(exit_date) != 'NaT':
                            sell_date_str = str(exit_date)[:10]
                        else:
                            # 兜底: 使用 actual_hold_days 估算卖出日期
                            from datetime import timedelta
                            buy_dt = pd.to_datetime(t_date_str)
                            sell_dt = buy_dt + timedelta(days=actual_hold_days)
                            sell_date_str = sell_dt.strftime('%Y-%m-%d')
                        
                        # 计算买卖价格和股数
                        buy_price = pick['close']
                        fwd_ret = pick['fwd_ret']
                        sell_price = buy_price * (1 + fwd_ret + self.slippage)  # 还原滑点
                        
                        # 计算实际买入股数 (按100股整手计算)
                        shares = int(position_value_per_stock / buy_price / 100) * 100
                        shares = max(100, shares)  # 至少买入100股
                        
                        # 实际仓位金额
                        actual_position_value = buy_price * shares
                        
                        # 获取操作标签
                        action_label = self._get_action_label(pick)
                        
                        self.trade_recorder.record_trade_pair(
                            buy_date=t_date_str,
                            sell_date=sell_date_str,
                            code=pick['code'],
                            name=pick['name'],
                            buy_price=buy_price,
                            sell_price=sell_price,
                            shares=shares,
                            exit_reason=pick.get('exit_reason', 'Time_Exit'),
                            sector=pick.get('sector', ''),
                            style_group=pick.get('style_group', ''),
                            alpha_score=pick.get('alpha_score', 0),
                            alpha_trend=pick.get('alpha_trend', 0),
                            mom_5=pick.get('mom_5_t', 0),
                            mom_20=pick.get('mom_20_t', 0),
                            sharpe=pick.get('sharpe_t', 0),
                            rsi=pick.get('rsi', 0),
                            bias_20=pick.get('bias_20', 0),
                            atr=pick.get('atr', 0),
                            actual_hold_days=actual_hold_days,
                            action_label=action_label,
                            position_value=actual_position_value
                        )
                
                # 持仓期收益复利到净值曲线
                equity_curve.append(equity_curve[-1] * (1 + period_ret))
                
                # 记录净值
                if self.record_trades and self.trade_recorder:
                    max_nav = max(equity_curve)
                    drawdown = (max_nav - equity_curve[-1]) / max_nav if max_nav > 0 else 0
                    bench_nav = bench_nav_map.get(t_date, 1.0)
                    
                    self.trade_recorder.record_equity(
                        trade_date=t_date_str,
                        nav=equity_curve[-1],
                        daily_return=period_ret,
                        cumulative_return=(equity_curve[-1] - 1) * 100,
                        drawdown=drawdown * 100,
                        position_count=len(picks),
                        benchmark_nav=bench_nav
                    )
                
                daily_stats.append({
                    'date': t_date, 
                    'ret': period_ret, 
                    'win_rate': daily_win,
                    'picks': len(picks)
                })
            else:
                equity_curve.append(equity_curve[-1])
                daily_stats.append({'date': t_date, 'ret': 0.0, 'win_rate': 0.0, 'picks': 0})

        return self._display_summary(equity_curve, daily_stats, trade_count, win_count)

    def _display_summary(self, curve: list, logs: list, trade_count: int = 0, win_count: int = 0) -> Dict:
        """显示回测报告"""
        
        # 防御性检查: 空数据处理
        if not logs or len(curve) < 2:
            logger.warning("[Backtest] 回测数据不足，无法生成报告")
            return {
                'hold_period': self.hold_period,
                'profit_pct': 0.0,
                'annual_ret': 0.0,
                'sharpe': 0.0,
                'win_rate': 0.0,
                'max_dd': 0.0,
                'final_nav': 1.0,
                'trade_count': 0
            }
        
        df_log = pd.DataFrame(logs)
        
        # 确保 ret 列存在
        if 'ret' not in df_log.columns or df_log['ret'].isna().all():
            logger.warning("[Backtest] 无有效收益数据")
            return {
                'hold_period': self.hold_period,
                'profit_pct': 0.0,
                'annual_ret': 0.0,
                'sharpe': 0.0,
                'win_rate': 0.0,
                'max_dd': 0.0,
                'final_nav': 1.0,
                'trade_count': 0
            }
        
        # 计算指标
        total_ret = (curve[-1] - 1) * 100
        max_vals = np.maximum.accumulate(curve)
        mdd = np.max((max_vals - curve) / (max_vals + 1e-9)) * 100
        
        # 夏普比率: 基于每期收益（每期 = hold_period 天）
        ret_std = df_log['ret'].std()
        ret_mean = df_log['ret'].mean()
        # 年化调整因子: 一年约 252/hold_period 期
        periods_per_year = 252 / self.hold_period
        sharpe = (ret_mean / (ret_std + 1e-9)) * np.sqrt(periods_per_year) if ret_std > 0 else 0.0
        
        # 胜率计算
        win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0
        avg_picks = df_log['picks'].mean() if 'picks' in df_log.columns else 0

        # 年化收益: 基于实际回测天数
        # 换仓次数 * hold_period = 实际交易天数
        actual_trading_days = self.backtest_days
        annual_ret = (curve[-1] ** (252 / actual_trading_days) - 1) * 100 if actual_trading_days > 0 else 0

        logger.info("=" * 60)
        logger.info(f"回测报告 | Hold={self.hold_period}天")
        logger.info(f"累计收益: {total_ret:.2f}% | 年化: {annual_ret:.2f}% | 净值: {curve[-1]:.4f}")
        logger.info(f"夏普: {sharpe:.2f} | 回撤: {mdd:.2f}% | 胜率: {win_rate:.1f}%")
        logger.info(f"交易次数: {trade_count} | 每期持仓: {avg_picks:.1f}只")
        logger.info("=" * 60)
        
        # 完成交易记录
        if self.record_trades and self.trade_recorder:
            self.trade_recorder.finalize(
                final_nav=curve[-1],
                total_return=total_ret,
                annual_return=annual_ret,
                sharpe_ratio=sharpe,
                max_drawdown=mdd,
                win_rate=win_rate,
                total_trades=trade_count
            )
            logger.info(f"[TradeRecorder] 交易记录已保存，会话ID: {self.trade_recorder.get_session_id()}")

        return {
            'hold_period': self.hold_period,
            'profit_pct': total_ret,
            'annual_ret': annual_ret,
            'sharpe': sharpe,
            'win_rate': win_rate,
            'max_dd': mdd,
            'final_nav': curve[-1],
            'trade_count': trade_count,
            'session_id': self.trade_recorder.get_session_id() if self.trade_recorder else None
        }


def run_sensitivity_analysis(
    days: Optional[int] = None, 
    periods: Optional[List[int]] = None,
    window_shift: int = 0
) -> Optional[pd.DataFrame]:
    """
    参数敏感性分析

    Args:
        days: 回测天数
        periods: 持仓周期列表
        window_shift: 窗口偏移天数 (用于回退到历史回测窗口)

    Returns:
        结果 DataFrame
    """
    from .. import config as cfg

    if periods is None:
        periods = [cfg.HOLD_PERIOD_DEFAULT]

    if days is None:
        days = cfg.BACKTEST_DAYS_DEFAULT

    logger.info("=" * 80)
    logger.info(f"Momentum 回测参数敏感性分析")
    logger.info(f"回测天数: {days} | 持仓周期: {periods}")
    logger.info("=" * 80)

    results = []

    for hp in periods:
        logger.info(f"\n{'='*40}")
        logger.info(f"Testing Hold Period: {hp} days")
        logger.info(f"{'='*40}")

        tester = MomentumBacktester(backtest_days=days, hold_period=hp, window_shift=window_shift)
        stats = tester.run_backtest()
        if stats:
            results.append(stats)

    if results:
        df_results = pd.DataFrame(results)
        print("\n" + "█" * 91)
        print(f"📊 Momentum 参数敏感性分析报告 (回测天数: {days})")
        print("-" * 91)
        print(f"{'持仓(天)':<12} {'收益%':<12} {'年化%':<12} {'夏普':<10} {'胜率%':<10} {'回撤%':<10} {'交易数':<8}")
        print("-" * 91)
        for _, r in df_results.iterrows():
            trades = r.get('trade_count', 0)
            print(
                f"{r['hold_period']:<12.0f} {r['profit_pct']:<12.2f} {r.get('annual_ret', 0):<12.2f} "
                f"{r['sharpe']:<10.2f} {r['win_rate']:<10.2f} {r['max_dd']:<10.2f} {trades:<8}"
            )

        best_profit = df_results.loc[df_results['profit_pct'].idxmax()]
        best_sharpe = df_results.loc[df_results['sharpe'].idxmax()]

        print("-" * 91)
        print(
            f"👑 收益最优: Hold={int(best_profit['hold_period'])}天 "
            f"(+{best_profit['profit_pct']:.1f}%)"
        )
        print(
            f"🛡️ 夏普最优: Hold={int(best_sharpe['hold_period'])}天 "
            f"(Sharpe={best_sharpe['sharpe']:.2f})"
        )
        print("█" * 91)

        return df_results

    return None
