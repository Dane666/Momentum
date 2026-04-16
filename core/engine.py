# -*- coding: utf-8 -*-
"""
Momentum 策略引擎
v16 工程化版本 - 集成连板择时、K线缓存、互联互通情绪
"""

import pandas as pd
import numpy as np
import time
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import logging

logger = logging.getLogger('momentum')


class MomentumEngine:
    """
    Momentum 策略引擎 - v16 工程化版本

    特性:
    - K线本地缓存
    - 连板高度择时
    - 互联互通情绪
    - 统一使用缓存版本计算因子
    """

    def __init__(self, watchlist: list = None, holdings: list = None, holding_costs: dict = None):
        """
        初始化引擎

        Args:
            watchlist: 观察列表
            holdings: 持仓列表
            holding_costs: 持仓成本字典 {代码: 买入价}
        """
        # 延迟导入避免循环依赖
        from .. import config as cfg
        from ..data import init_db

        # 性能参数
        self.max_io_workers = cfg.MAX_IO_WORKERS
        self.max_ai_workers = cfg.MAX_AI_WORKERS

        # 策略参数
        self.watchlist = watchlist or []
        self.holdings = holdings or []
        self.holding_costs = holding_costs or {}  # 买入价字典

        # 风控阈值
        self.RSI_DANGER_ZONE = cfg.RSI_DANGER_ZONE
        self.VOL_SURGE_LIMIT = cfg.VOL_SURGE_LIMIT

        # 缓存
        self.sector_cache = {}
        self.holder_cache = {}
        self.bill_cache = {}

        # 市场状态
        self.market_total_amount = 0.0
        self.market_breadth = 0.0
        self.streak_emotion = 'NORMAL'
        self.connect_trend = 'NEUTRAL'
        self.position_multiplier = 1.0
        self.dxy_val = 100.0
        self.dxy_trend = '平稳'

        # 初始化数据库
        init_db()

        logger.info("[Engine] Momentum 引擎初始化完成")

    def calculate_indicators_cached(self, code: str, name: str, mkt_cap: float = 0, skip_nlp: bool = False) -> Optional[dict]:
        """
        使用本地缓存的K线计算指标 (统一入口)

        Args:
            code: 股票代码
            name: 股票名称
            mkt_cap: 市值
            skip_nlp: 是否跳过NLP分析 (用于两阶段筛选优化)

        Returns:
            因子字典，失败返回 None
        """
        from .. import config as cfg
        from ..data import load_or_fetch_kline, fetch_kline_from_api, fetch_stock_concept
        from ..factors import get_style_group, compute_dual_day_factors

        try:
            # 1. 使用缓存优先加载K线
            df = load_or_fetch_kline(code, fetch_kline_from_api, cfg.KLINE_START_DATE)

            if df is None or len(df) < 35:
                return None

            # 2. 数据预处理
            df = df.copy()
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
            df['turnover_rate'] = pd.to_numeric(
                df.get('turnover_ratio', df.get('turnover_rate', 0)),
                errors='coerce'
            ).fillna(0)
            df = df.sort_values('trade_date').reset_index(drop=True)

            # 3. 计算双日因子
            today, yesterday = compute_dual_day_factors(df)
            if today is None:
                return None

            # 4. 获取板块信息
            if code not in self.sector_cache:
                self.sector_cache[code] = fetch_stock_concept(code)

            # 5. 获取大单数据
            big_order_t, big_order_y = self._get_dual_big_order_factors(code, df)

            # 6. 获取筹码集中度
            chip_rate = self._get_chip_concentration(code)

            # 7. NLP 分析 (如果启用且未跳过)
            nlp_score = 0.0
            if not skip_nlp and getattr(cfg, 'ENABLE_NLP_ANALYSIS', False):
                try:
                    from ..factors.nlp import analyze_sentiment
                    score, _ = analyze_sentiment(code, self.dxy_val, self.dxy_trend)
                    nlp_score = score
                except Exception as e:
                    logger.warning(f"[NLP] {code} 分析失败: {e}")

            # 8. 庄股识别因子 (如果启用)
            manipulation_score = 0.0
            momentum_r2 = 0.0
            ivol = 0.0
            illiq = 0.0
            overnight_ratio = 0.0
            
            if getattr(cfg, 'ENABLE_MANIPULATION_FILTER', False) and len(df) >= 20:
                try:
                    from ..factors.quant_factors import QuantFactors
                    # 构建因子计算所需的DataFrame格式
                    df_calc = df.tail(30).copy()
                    df_calc = df_calc.rename(columns={
                        'close': 'Close', 'open': 'Open', 
                        'high': 'High', 'low': 'Low', 'volume': 'Volume'
                    })
                    if 'Open' not in df_calc.columns:
                        df_calc['Open'] = df_calc['Close']
                    
                    qf = QuantFactors(df_calc, window=20)
                    manipulation_score = qf.calc_manipulation_score().iloc[-1]
                    factors = qf.calc_all_factors()
                    if not factors.empty:
                        momentum_r2 = factors['Momentum_Quality_R2'].iloc[-1]
                        ivol = factors['IVOL'].iloc[-1]
                        illiq = factors['Amihud_Illiquidity'].iloc[-1]
                        overnight_ratio = factors['Overnight_Ratio'].iloc[-1]
                except Exception as e:
                    logger.debug(f"[Manipulation] {code} 因子计算失败: {e}")

            # 9. 构建返回结果
            return {
                'code': code,
                'name': name,
                'sector': self.sector_cache.get(code, "其它"),
                'style_group': get_style_group(mkt_cap),
                'close': today['close'],
                'rsi': today['rsi'],
                'atr': today['atr'],
                'ma5': today['ma5'],
                'ma20': today['ma20'],
                'chip_rate': chip_rate,
                'big_order_t': big_order_t,
                'big_order_y': big_order_y,
                # 今日因子
                'mom_5_t': today['mom_5'],
                'mom_20_t': today['mom_20'],
                'sharpe_t': today['sharpe'],
                'vr_t': today['vol_ratio'],
                'turnover_t': today['turnover'],
                # 昨日因子
                'mom_5_y': yesterday['mom_5'] if yesterday else 0,
                'mom_20_y': yesterday['mom_20'] if yesterday else 0,
                'sharpe_y': yesterday['sharpe'] if yesterday else 0,
                'vr_y': yesterday['vol_ratio'] if yesterday else 0,
                # 其他
                'hk_bonus': 0.0,
                'nlp_score': nlp_score,
                'change_pct': today['change_pct'],
                # 庄股识别因子
                'manipulation_score': manipulation_score,
                'momentum_r2': momentum_r2,
                'ivol': ivol,
                'illiq': illiq,
                'overnight_ratio': overnight_ratio,
            }

        except Exception as e:
            logger.warning(f"[Engine] {code} 指标计算失败: {e}")
            return None

    def _get_chip_concentration(self, code: str) -> float:
        """获取筹码集中度 (股东人数变化率)"""
        if code in self.holder_cache:
            return self.holder_cache[code]

        try:
            import requests

            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                "reportName": "RPT_HOLDERNUMLATEST",
                "columns": "SECURITY_CODE,END_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_RATIO",
                "filter": f'(SECURITY_CODE="{code}")',
                "pageIndex": "1",
                "pageSize": "1",
                "sortColumns": "END_DATE",
                "sortChars": "-1",
                "source": "WEB",
                "client": "WEB"
            }

            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Referer": "https://data.eastmoney.com/gdhs/"
            }

            response = requests.get(url, params=params, headers=headers, timeout=3)
            res_json = response.json()

            if (res_json and "result" in res_json and
                res_json["result"] and "data" in res_json["result"]):
                data = res_json["result"]["data"][0]
                raw_ratio = float(data.get('HOLDER_NUM_RATIO', 0))
                change_rate = raw_ratio / 100.0
                self.holder_cache[code] = change_rate
                return change_rate

        except Exception:
            pass

        return 0.0

    def _get_dual_big_order_factors(self, code: str, df_market: pd.DataFrame) -> tuple:
        """
        获取今日和昨日的主力大单占比（百分比值）
        
        数据源: efinance
        - 历史数据: 直接使用官方计算的占比字段
        - 今日实时: 使用金额数据，通过实时成交额计算占比
        
        Returns:
            (big_order_t, big_order_y): 今日和昨日大单占比百分比值（如 -3.14 表示 -3.14%）
        """
        from ..data import is_trading_hours
        
        try:
            import efinance as ef

            # 获取历史资金流 (始终可用)
            df_hist_bill = ef.stock.get_history_bill(code)
            if df_hist_bill is None or df_hist_bill.empty:
                return 0.0, 0.0

            def get_hist_ratio(bill_row):
                """从历史资金流提取大单占比（使用官方百分比字段）"""
                try:
                    ratio_big = float(bill_row['大单流入净占比'])
                    ratio_super = float(bill_row['超大单流入净占比'])
                    return ratio_big + ratio_super
                except:
                    return 0.0

            # 盘中 vs 盘后处理
            big_order_t = 0.0
            big_order_y = 0.0
            
            if is_trading_hours():
                # 盘中: 
                # - 昨日用历史最后一条（有官方占比）
                # - 今日用实时数据计算
                big_order_y = get_hist_ratio(df_hist_bill.iloc[-1])
                
                try:
                    df_today_bill = ef.stock.get_today_bill(code)
                    if df_today_bill is not None and not df_today_bill.empty:
                        last_bill = df_today_bill.iloc[-1]
                        inflow_big = float(last_bill['大单净流入'])
                        inflow_super = float(last_bill['超大单净流入'])
                        total_inflow = inflow_big + inflow_super
                        
                        # 获取实时成交额（从实时行情缓存）
                        # 如果有缓存的实时行情，使用真实成交额
                        t_amount = 0.0
                        if hasattr(self, 'realtime_quotes') and self.realtime_quotes is not None:
                            rt_row = self.realtime_quotes[self.realtime_quotes['股票代码'] == code]
                            if not rt_row.empty:
                                t_amount = float(rt_row['成交额'].iloc[0])
                        
                        # 如果没有实时数据，使用估算值（但会不准确）
                        if t_amount <= 0:
                            t_amount = df_market['amount'].iloc[-1] if len(df_market) >= 1 else 1e8
                        
                        if t_amount > 0:
                            big_order_t = (total_inflow / t_amount) * 100
                except Exception:
                    pass
            else:
                # 盘后: 历史数据已更新
                # - 今日是历史最后一条
                # - 昨日是历史倒数第二条
                if len(df_hist_bill) >= 1:
                    big_order_t = get_hist_ratio(df_hist_bill.iloc[-1])
                if len(df_hist_bill) >= 2:
                    big_order_y = get_hist_ratio(df_hist_bill.iloc[-2])

            return big_order_t, big_order_y

        except Exception as e:
            logger.debug(f"[BigOrder] {code} 获取失败: {e}")
            return 0.0, 0.0

    def industry_neutralization_with_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        风格中性化与Alpha合成
        
        委托给 alpha 模块统一实现，确保与回测逻辑一致。
        """
        from ..alpha import AlphaModel
        
        if df.empty:
            return df
        
        # 使用统一的 Alpha 模型
        model = AlphaModel(
            market_total_amount=self.market_total_amount,
            vol_surge_limit=self.VOL_SURGE_LIMIT
        )
        return model.neutralize_and_score(df)

    def run_all_market_scan_pro(self) -> pd.DataFrame:
        """
        增强版市场扫描 - 集成连板择时和互联互通情绪
        
        委托给 MarketScanner 模块实现。
        """
        from .scanner import MarketScanner
        scanner = MarketScanner(self)
        return scanner.scan()

    def _check_backtest_buy_criteria(self, row) -> tuple:
        """
        检查是否符合回测买入标准
        
        Returns:
            (is_qualified: bool, reasons: list) - 是否达标及不达标原因
        """
        from .. import config as cfg
        
        reasons = []
        
        # 条件1: RSI 不能过高
        rsi_limit = getattr(cfg, 'RSI_DANGER_ZONE', 80.0)
        if row.get('rsi', 0) > rsi_limit:
            reasons.append(f"RSI>{rsi_limit:.0f}")
        
        # 条件2: 价格站上 MA20
        if row.get('close', 0) <= row.get('ma20', float('inf')):
            reasons.append("<MA20")
        
        # 条件3: Sharpe 达标
        if row.get('sharpe_t', 0) <= cfg.MIN_SHARPE:
            reasons.append(f"Sharpe<{cfg.MIN_SHARPE}")

        # 条件4: 乖离率不过热
        bias_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20)
        ma20 = row.get('ma20', 0)
        close = row.get('close', 0)
        if ma20 > 0:
            bias_20 = (close / ma20) - 1
            if bias_20 >= bias_limit:
                reasons.append(f"乖离>{bias_limit*100:.0f}%")
        
        is_qualified = len(reasons) == 0
        return is_qualified, reasons

    def _get_action_label(self, row) -> str:
        """获取操作建议标签 (结合回测买入标准)"""
        is_bt_qualified, bt_reasons = self._check_backtest_buy_criteria(row)
        
        # 回测达标 + 高 Alpha 分 = 强买入信号
        if is_bt_qualified:
            if row['alpha_score'] > 1.2 and row['alpha_trend'] > 0:
                return "🎯 [回测买入]"  # 完全符合回测条件的强信号
            elif row['alpha_score'] > 0.8 and row['alpha_trend'] > 0.3:
                return "✅ [回测买入]"  # 符合回测条件的次强信号
            else:
                return "📊 [回测达标]"  # 符合回测条件但信号一般
        else:
            # 不符合回测条件，显示原因
            reason_str = ",".join(bt_reasons)
            if row['alpha_score'] > 1.2 and row['alpha_trend'] > 0 and row['close'] > row['ma20']:
                return f"⚡ [黄金观察] ({reason_str})"
            elif row['alpha_score'] > 0.8:
                return f"🥈 [白银观察] ({reason_str})"
            else:
                return f"📈 [观察] ({reason_str})"

    def _display_report(self, df: pd.DataFrame, start_t: float, filter_stats: dict = None) -> str:
        """显示策略报告并返回报告文本"""
        from ..notify import send_feishu_msg
        from .. import config as cfg

        report_lines = []

        # 头部
        header = (f"Momentum v16 [工程化版] | 成交额: {self.market_total_amount/1e8:.0f}亿 | "
                  f"情绪: {self.streak_emotion} | 外资: {self.connect_trend} | "
                  f"仓位系数: {self.position_multiplier:.1f}x")
        report_lines.append(header)
        report_lines.append("-" * 130)
        
        # 显示过滤漏斗统计
        if filter_stats:
            report_lines.append("🔬 选股漏斗:")
            funnel_parts = []
            for key, val in filter_stats.items():
                # 跳过内部标记字段
                if key.startswith('_') or key in ['量比跳过']:
                    continue
                if isinstance(val, int):
                    funnel_parts.append(f"{key}({val})")
            report_lines.append("   " + " → ".join(funnel_parts))
            report_lines.append("-" * 130)

        if df.empty:
            report_lines.append("今日无推荐标的")
            report_lines.append("")
            report_lines.append("📋 可能原因:")
            report_lines.append(f"   • 涨幅范围: {cfg.MIN_CHANGE_PCT}% ~ {cfg.MAX_CHANGE_PCT}% 内股票数量不足")
            report_lines.append(f"   • 量比要求: ≥{cfg.MIN_VOL_RATIO} (刚开盘数据可能不准)")
            report_lines.append(f"   • RSI限制: <{cfg.RSI_DANGER_ZONE} (避免追高)")
            report_lines.append(f"   • 夏普要求: >{cfg.MIN_SHARPE}")
        else:
            # 【修改】只显示回测达标的股票
            bt_qualified_rows = [r for _, r in df.iterrows() if self._check_backtest_buy_criteria(r)[0]]
            
            if not bt_qualified_rows:
                report_lines.append("今日无回测达标标的")
                report_lines.append("")
                report_lines.append("📋 可能原因:")
                report_lines.append("   • 候选股票虽符合涨幅/量比/成交额要求，但未达到回测买入标准")
                report_lines.append("   • 回测标准: Alpha>0 且 站上MA20 且 乖离率<20%")
            else:
                # 计算仓位建议 (与回测一致的等权分配逻辑)
                initial_capital = getattr(cfg, 'INITIAL_CAPITAL', 100000.0)
                num_qualified = len(bt_qualified_rows)
                position_per_stock = initial_capital / num_qualified if num_qualified > 0 else 0
                
                # 获取止损参数
                fixed_stop_pct = getattr(cfg, 'FIXED_STOP_PCT', 0.07)  # 固定止损7%
                
                # 表头
                report_lines.append(
                    f"{self._pad_str('代码', 8)} {self._pad_str('名称', 10)} {self._pad_str('现价', 8)} "
                    f"{self._pad_str('MA5止盈', 9)} {self._pad_str('固定止损', 10)} {self._pad_str('建议股数', 10)} "
                    f"{self._pad_str('仓位', 10)} {self._pad_str('Alpha', 7)} {'状态'}"
                )

                for r in bt_qualified_rows:
                    current_price = r.get('close', r.get('price', 0))
                    ma5 = r.get('ma5', current_price * 0.98)  # MA5作为趋势止盈位
                    
                    # 固定百分比止损 (买入价的-7%)
                    fixed_stop_price = current_price * (1 - fixed_stop_pct)
                    
                    if current_price > 0:
                        # 按100股整手计算
                        suggested_shares = int(position_per_stock / current_price / 100) * 100
                        suggested_shares = max(100, suggested_shares)
                        actual_position = current_price * suggested_shares
                        shares_str = f"{suggested_shares}股"
                        position_str = f"¥{actual_position/10000:.2f}万"
                        ma5_str = f"¥{ma5:.2f}"
                        stop_str = f"¥{fixed_stop_price:.2f}(-{fixed_stop_pct*100:.0f}%)"
                    else:
                        shares_str = "-"
                        position_str = "-"
                        ma5_str = "-"
                        stop_str = "-"
                    
                    line = (f"{self._pad_str(str(r['code']), 8)} {self._pad_str(str(r['name']), 10)} "
                            f"{current_price:<8.2f} {self._pad_str(ma5_str, 9)} {self._pad_str(stop_str, 10)} "
                            f"{self._pad_str(shares_str, 10)} {self._pad_str(position_str, 10)} "
                            f"{r['alpha_score']:<7.2f} {r.get('action', '')}")
                    report_lines.append(line)
                
                # 显示每只股票的达标指标明细
                report_lines.append("")
                report_lines.append("📋 达标指标明细:")
                for r in bt_qualified_rows:
                    code = r['code']
                    name = r['name']
                    # 收集达标指标
                    indicators = []
                    indicators.append(f"涨幅{r.get('change_pct', 0):.1f}%")
                    indicators.append(f"量比{r.get('vr_t', 0):.1f}")
                    indicators.append(f"RSI={r.get('rsi', 0):.0f}")
                    indicators.append(f"夏普={r.get('sharpe_t', 0):.2f}")
                    indicators.append(f"Alpha={r.get('alpha_score', 0):.2f}")
                    indicators.append(f"趋势={r.get('alpha_trend', 0):+.2f}")
                    ma_status = "站上MA20✓" if r.get('close', 0) > r.get('ma20', 0) else "破MA20✗"
                    indicators.append(ma_status)
                    report_lines.append(f"   {code} {name}: {' | '.join(indicators)}")
                
                # 显示自适应止损建议
                report_lines.append("")
                report_lines.append("🔄 自适应止损建议:")
                try:
                    from ..risk.adaptive_exit import AdaptiveExitEngine
                    adaptive_engine = AdaptiveExitEngine()
                    
                    for r in bt_qualified_rows:
                        code = r['code']
                        name = r['name']
                        current_price = r.get('close', r.get('price', 0))
                        rsi = r.get('rsi', 50)
                        ma20 = r.get('ma20', current_price)
                        bias = (current_price / ma20 - 1) if ma20 > 0 else 0
                        atr_pct = r.get('atr_pct', 1.5)  # ATR占价格百分比
                        
                        # 获取自适应参数
                        params = adaptive_engine.get_adaptive_params(
                            atr_pct=atr_pct,
                            rsi=rsi,
                            bias=bias,
                            market_condition='normal'
                        )
                        
                        # 计算具体价位
                        stop_price = current_price * (1 - params.stop_loss_pct)
                        profit_price = current_price * (1 + params.take_profit_pct)
                        
                        report_lines.append(
                            f"   {code} {name}: 止损¥{stop_price:.2f}(-{params.stop_loss_pct:.0%}) | "
                            f"止盈¥{profit_price:.2f}(+{params.take_profit_pct:.0%}) | "
                            f"原因: {params.reason}"
                        )
                except Exception as e:
                    report_lines.append(f"   (自适应模块加载失败: {e})")
                
                # 回测买入统计和仓位汇总
                report_lines.append("")
                report_lines.append(f"📊 回测买入达标: {num_qualified} 只 (仅显示 📊 标记)")
                if num_qualified > 0:
                    report_lines.append(f"💰 建议资金: ¥{initial_capital/10000:.1f}万 | 等权分配: ¥{position_per_stock/10000:.2f}万/只")
                    take_profit_pct = getattr(cfg, 'TAKE_PROFIT_PCT', 0.10)
                    report_lines.append(
                        f"🛡️ 统一风控: 固定止盈+{take_profit_pct*100:.0f}% / 固定止损-{fixed_stop_pct*100:.0f}% / 破MA5、乖离、RSI、破MA20监控"
                    )
                    report_lines.append("   💡 提示: 🎯=强买入 | ✅=次强 | 📊=达标 | 建议14:50尾盘买入")
                
                # 显示选股门槛
                report_lines.append("")
                bias_limit_pct = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20) * 100
                report_lines.append(f"🔍 选股门槛: 涨幅{cfg.MIN_CHANGE_PCT}~{cfg.MAX_CHANGE_PCT}% | 量比≥{cfg.MIN_VOL_RATIO} | RSI<{cfg.RSI_DANGER_ZONE} | 夏普>{cfg.MIN_SHARPE} | 站上MA20 | 乖离<{bias_limit_pct:.0f}%")

        report_lines.append(f"\n[耗时]: {time.time() - start_t:.1f}s")

        report_text = "\n".join(report_lines)

        # 控制台输出
        print("\n" + "█" * 100)
        for line in report_lines:
            print(line)
        print("█" * 100)

        # 飞书通知
        send_feishu_msg("Momentum 策略报告", report_text)

        return report_text

    def portfolio_realtime_monitor(self) -> pd.DataFrame:
        """
        实时持仓诊断
        
        委托给 PortfolioMonitor 模块实现。
        """
        from .monitor import PortfolioMonitor
        monitor = PortfolioMonitor(self)
        return monitor.monitor()

    def _logic_exit_check(self, row) -> str:
        """
        持仓离场逻辑判定
        
        委托给 risk 模块统一实现，确保与回测逻辑一致。
        """
        from ..risk import check_realtime_exit

        code = str(row.get('code', ''))
        cost_price = self.holding_costs.get(code, 0)
        
        result = check_realtime_exit(row, cost_price)
        return result.action

    @staticmethod
    def _pad_str(s: str, width: int, align: str = '<') -> str:
        """
        处理包含中文字符的字符串填充对齐
        Args:
            s: 原字符串
            width: 目标显示宽度
            align: 对齐方式 ('<', '>', '^')
        """
        # 计算显示宽度：中文字符算2，其他算1
        display_width = 0
        for char in s:
            if ord(char) > 127:
                display_width += 2
            else:
                display_width += 1
        
        padding = max(0, width - display_width)
        
        if align == '<':
            return s + ' ' * padding
        elif align == '>':
            return ' ' * padding + s
        else: # center
            left = padding // 2
            right = padding - left
            return ' ' * left + s + ' ' * right

    def _display_portfolio_report(self, df: pd.DataFrame) -> str:
        """显示持仓诊断报告并返回报告文本"""
        from ..notify import send_feishu_msg
        from .. import config as cfg

        report_lines = []

        # 头部
        # 添加时间信息，避免 AI 判断错误
        from datetime import datetime
        now = datetime.now()
        time_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # 判断市场状态
        hour = now.hour
        if hour < 9 or (hour == 9 and now.minute < 30):
            market_phase = "盘前"
        elif hour < 11 or (hour == 11 and now.minute < 30):
            market_phase = "上午盘中(成交数据不完整)"
        elif hour < 13:
            market_phase = "午间休市(成交为上午数据)"
        elif hour < 15:
            market_phase = "下午盘中" if hour < 14 or now.minute < 30 else "尾盘(数据接近完整)"
        else:
            market_phase = "盘后(全天完整数据)"
        
        # 获取止损止盈参数
        fixed_stop_pct = getattr(cfg, 'FIXED_STOP_PCT', 0.05)  # 固定止损
        take_profit_pct = getattr(cfg, 'TAKE_PROFIT_PCT', 0.10)  # 固定止盈
        bias_profit_limit = getattr(cfg, 'BIAS_PROFIT_LIMIT', 0.20)  # 乖离率止盈
        rsi_danger_zone = getattr(cfg, 'RSI_DANGER_ZONE', 80.0)  # RSI超买区
        use_adaptive = getattr(cfg, 'USE_ADAPTIVE_EXIT', True)  # 自适应止损模式
        
        # 初始化自适应引擎
        adaptive_engine = None
        if use_adaptive:
            try:
                from ..risk.adaptive_exit import AdaptiveExitEngine
                adaptive_engine = AdaptiveExitEngine()
            except ImportError:
                pass
        
        adaptive_mode_str = "✅ 自适应止损(已开启)" if use_adaptive else "❌ 固定止损模式"
        header = f"持仓监控报告 | 时间: {time_str} | {market_phase} | 成交额: {self.market_total_amount/1e8:.0f}亿 | {adaptive_mode_str}"
        report_lines.append(header)
        report_lines.append("-" * 180)
        
        # 止盈止损规则说明
        report_lines.append("📋 止盈止损规则 (优先级从高到低):")
        report_lines.append(f"   ① 固定止盈: 触及买入价×{(1+take_profit_pct)*100:.0f}% → 立即止盈")
        if use_adaptive:
            report_lines.append(f"   ② 自适应止损: 根据波动率/RSI/乖离率动态调整 (基准{fixed_stop_pct*100:.0f}%)")
        else:
            report_lines.append(f"   ② 固定止损: 跌破买入价×{(1-fixed_stop_pct)*100:.0f}% → 立即清仓")
        report_lines.append(f"   ③ MA5止盈: 收盘跌破MA5 → 趋势结束离场")
        report_lines.append(f"   ④ 乖离率止盈: 偏离MA20超{bias_profit_limit*100:.0f}% → 冲高回落风险，止盈")
        report_lines.append(f"   ⑤ RSI止盈: RSI>{rsi_danger_zone:.0f} → 超买区域，分批止盈")
        report_lines.append("   ⑥ 破MA20清仓: 收盘跌破MA20 → 趋势反转，清仓")
        report_lines.append("-" * 180)

        # 表头
        if use_adaptive:
            report_lines.append(
                f"{self._pad_str('代码', 8)} {self._pad_str('买入价', 8)} {self._pad_str('现价', 8)} "
                f"{self._pad_str('盈亏%', 8)} {self._pad_str('固定止损', 9)} {self._pad_str('适应止损', 9)} "
                f"{self._pad_str('MA5', 8)} {self._pad_str('乖离%', 7)} {self._pad_str('RSI', 6)} "
                f"{self._pad_str('止盈信号', 12)} {self._pad_str('适应调整', 20)} {'操作建议'}"
            )
        else:
            report_lines.append(
                f"{self._pad_str('代码', 8)} {self._pad_str('买入价', 8)} {self._pad_str('现价', 8)} "
                f"{self._pad_str('盈亏%', 8)} {self._pad_str('止损价', 9)} {self._pad_str('MA5', 8)} "
                f"{self._pad_str('乖离%', 7)} {self._pad_str('RSI', 6)} {self._pad_str('MA20', 8)} "
                f"{self._pad_str('止盈信号', 12)} {'操作建议'}"
            )
        report_lines.append("-" * 180)

        for _, r in df.sort_values('alpha_score', ascending=False).iterrows():
            code = str(r['code'])
            current_price = r['close']
            ma5 = r.get('ma5', current_price * 0.98)
            ma20 = r.get('ma20', current_price * 0.95)
            rsi = r.get('rsi', 50)
            atr = r.get('atr', current_price * 0.02)  # 默认ATR为价格的2%
            
            # 计算乖离率 (相对MA20)
            bias_20 = ((current_price / ma20) - 1) if ma20 > 0 else 0
            bias_str = f"{bias_20*100:+.1f}%"
            
            # ATR百分比
            atr_pct = (atr / current_price) * 100 if current_price > 0 else 1.5
            
            # 获取买入价
            cost_price = self.holding_costs.get(code, 0)
            if cost_price > 0:
                cost_str = f"{cost_price:.2f}"
                pnl_pct = ((current_price / cost_price) - 1) * 100
                pnl_str = f"{pnl_pct:+.1f}%"
                # 基于买入价计算固定止损位
                stop_price = cost_price * (1 - fixed_stop_pct)
                stop_str = f"{stop_price:.2f}"
                take_profit_price = cost_price * (1 + take_profit_pct)
                
                # 自适应止损计算
                adaptive_stop_str = "-"
                adaptive_reason = ""
                if use_adaptive and adaptive_engine:
                    adaptive_params = adaptive_engine.get_adaptive_params(
                        atr_pct=atr_pct,
                        rsi=rsi,
                        bias=bias_20,
                        market_condition='normal',
                        entry_price=cost_price,
                        current_price=current_price
                    )
                    adaptive_stop_pct = adaptive_params.stop_loss_pct
                    adaptive_stop_price = cost_price * (1 - adaptive_stop_pct)
                    adaptive_stop_str = f"{adaptive_stop_price:.2f}"
                    adaptive_reason = adaptive_params.reason[:18] if len(adaptive_params.reason) > 18 else adaptive_params.reason
            else:
                cost_str = "-"
                pnl_pct = 0
                pnl_str = "-"
                stop_price = 0
                stop_str = "-"
                take_profit_price = 0
                adaptive_stop_str = "-"
                adaptive_reason = ""
            
            # 止盈信号判断
            profit_signals = []
            
            # ① 固定止盈
            if cost_price > 0 and current_price >= take_profit_price:
                profit_signals.append("止盈💰")

            # ② 固定止损
            if cost_price > 0 and current_price <= stop_price:
                profit_signals.append("止损🚨")

            # ③ MA5止盈
            if current_price < ma5:
                profit_signals.append("破MA5🚨")

            # ④ 乖离率止盈
            if bias_20 >= bias_profit_limit:
                profit_signals.append("乖离⚠️")

            # ⑤ RSI止盈
            if rsi >= rsi_danger_zone:
                profit_signals.append("RSI⚠️")

            # ⑥ MA20破位
            if current_price < ma20:
                profit_signals.append("破MA20🚨")
            
            # 综合止盈信号
            if not profit_signals:
                signal_str = "持有✅"
            else:
                signal_str = " ".join(profit_signals[:2])  # 最多显示2个信号
            
            if use_adaptive:
                line = (
                    f"{self._pad_str(code, 8)} {self._pad_str(cost_str, 8)} {current_price:<8.2f} "
                    f"{self._pad_str(pnl_str, 8)} {self._pad_str(stop_str, 9)} {self._pad_str(adaptive_stop_str, 9)} "
                    f"{ma5:<8.2f} {self._pad_str(bias_str, 7)} {rsi:<6.1f} "
                    f"{self._pad_str(signal_str, 12)} {self._pad_str(adaptive_reason, 20)} {r['action']}"
                )
            else:
                line = (
                    f"{self._pad_str(code, 8)} {self._pad_str(cost_str, 8)} {current_price:<8.2f} "
                    f"{self._pad_str(pnl_str, 8)} {self._pad_str(stop_str, 9)} {ma5:<8.2f} "
                    f"{self._pad_str(bias_str, 7)} {rsi:<6.1f} {ma20:<8.2f} "
                    f"{self._pad_str(signal_str, 12)} {r['action']}"
                )
            report_lines.append(line)

        report_lines.append("=" * 180)
        report_lines.append("💡 信号说明: 🚨=立即行动 ⚠️=警惕观察 ✅=持有")
        if use_adaptive:
            report_lines.append("📊 自适应止损: 根据ATR波动率、RSI、乖离率、浮盈状态动态调整止损位(夏普+1.38验证)")

        report_text = "\n".join(report_lines)

        # 控制台输出
        print("\n" + "💼" * 15 + " 持仓监控报告 " + "💼" * 15)
        for line in report_lines:
            print(line)
        print("█" * 180)

        # 飞书通知
        send_feishu_msg("持仓诊断报告", report_text)

        return report_text

    # ==================== ETF 诊断模块 ====================

    def etf_realtime_monitor(self, etf_holdings: list = None) -> tuple:
        """
        ETF 实时持仓诊断

        Args:
            etf_holdings: ETF 持仓代码列表

        Returns:
            (DataFrame, 报告文本)
        """
        from ..data import fetch_realtime_quotes

        if not etf_holdings:
            logger.warning("[ETF] 未配置 ETF 持仓列表")
            return pd.DataFrame(), ""

        logger.info("=" * 60)
        logger.info("[ETF诊断] ETF 实时持仓分析")
        logger.info("=" * 60)

        # 获取 ETF 实时行情
        df_etf = fetch_realtime_quotes(fs='ETF')
        if df_etf is None or df_etf.empty:
            logger.error("获取 ETF 实时行情失败")
            return pd.DataFrame(), ""

        # 获取持仓 ETF 信息
        df_etf['股票代码'] = df_etf['股票代码'].astype(str)
        df_hold = df_etf[df_etf['股票代码'].isin(etf_holdings)].copy()

        if df_hold.empty:
            logger.warning("[ETF] 持仓 ETF 不在行情数据中")
            return pd.DataFrame(), ""

        # 计算 ETF 因子
        results = []
        for _, row in df_hold.iterrows():
            code = row['股票代码']
            name = row['股票名称']
            indicators = self._calculate_etf_indicators(code, name, row, df_etf)
            if indicators:
                results.append(indicators)

        if not results:
            logger.warning("[ETF] 无法计算 ETF 因子")
            return pd.DataFrame(), ""

        df_result = pd.DataFrame(results)

        # 执行离场逻辑判定
        df_result['action'] = df_result.apply(self._logic_etf_exit_check, axis=1)

        # 打印报告
        report_text = self._display_etf_report(df_result)

        return df_result, report_text

    def _calculate_etf_indicators(self, code: str, name: str, realtime_row: pd.Series, df_all_etf: pd.DataFrame) -> Optional[dict]:
        """
        计算 ETF 特有因子

        Args:
            code: ETF 代码
            name: ETF 名称
            realtime_row: 实时行情 Series
            df_all_etf: 全市场 ETF 行情 DataFrame

        Returns:
            因子字典，失败返回 None
        """
        from .. import config as cfg
        from ..data import load_or_fetch_kline, fetch_kline_from_api

        try:
            # 1. 解析实时数据
            close = float(realtime_row.get('最新价', 0))
            change_pct = float(realtime_row.get('涨跌幅', 0))
            amount = float(realtime_row.get('成交额', 0))
            volume = float(realtime_row.get('成交量', 0))
            vol_ratio = float(realtime_row.get('量比', 0)) if '量比' in realtime_row else 0

            # 2. 获取 K 线数据计算技术指标
            df = load_or_fetch_kline(code, fetch_kline_from_api, cfg.KLINE_START_DATE)

            ma5, ma20, rsi, atr = 0, 0, 50, 0
            turnover_5d_avg = 0

            if df is not None and len(df) >= 20:
                df = df.copy()
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                df['high'] = pd.to_numeric(df['high'], errors='coerce')
                df['low'] = pd.to_numeric(df['low'], errors='coerce')
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
                df = df.sort_values('trade_date').reset_index(drop=True)

                # MA5 / MA20
                ma5 = df['close'].iloc[-5:].mean() if len(df) >= 5 else close
                ma20 = df['close'].iloc[-20:].mean() if len(df) >= 20 else close

                # RSI (14日)
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / (loss + 1e-9)
                rsi_series = 100 - (100 / (1 + rs))
                rsi = rsi_series.iloc[-1] if not rsi_series.empty else 50

                # ATR (14日)
                tr = pd.concat([
                    df['high'] - df['low'],
                    (df['high'] - df['close'].shift(1)).abs(),
                    (df['low'] - df['close'].shift(1)).abs()
                ], axis=1).max(axis=1)
                atr = tr.rolling(14).mean().iloc[-1] if len(tr) >= 14 else 0

                # 5日平均成交额
                turnover_5d_avg = df['amount'].iloc[-5:].mean() if len(df) >= 5 else amount

            # 3. 计算成交额排名 (在全市场 ETF 中的位置)
            df_all_etf['成交额'] = pd.to_numeric(df_all_etf['成交额'], errors='coerce').fillna(0)
            total_etf = len(df_all_etf)
            amount_rank = (df_all_etf['成交额'] >= amount).sum()
            amount_rank_pct = amount_rank / total_etf if total_etf > 0 else 0.5

            # 4. 资金净流入 (efinance, 区分盘中/盘后)
            net_inflow = 0
            try:
                from ..data import is_trading_hours
                import efinance as ef
                
                if is_trading_hours():
                    # 盘中: 获取今日实时资金流
                    df_bill = ef.stock.get_today_bill(code)
                    if df_bill is not None and not df_bill.empty:
                        last_row = df_bill.iloc[-1]
                        net_inflow = float(last_row.get('主力净流入', 0))
                else:
                    # 盘后: 获取历史资金流最后一条
                    df_hist_bill = ef.stock.get_history_bill(code)
                    if df_hist_bill is not None and not df_hist_bill.empty:
                        last_row = df_hist_bill.iloc[-1]
                        net_inflow = float(last_row.get('主力净流入', 0))
            except Exception:
                pass

            # 5. 量能变化 (今日 vs 5日均量)
            vol_change = (amount / turnover_5d_avg - 1) if turnover_5d_avg > 0 else 0

            return {
                'code': code,
                'name': name,
                'close': close,
                'change_pct': change_pct,
                'amount': amount,
                'amount_rank_pct': amount_rank_pct,
                'vol_ratio': vol_ratio,
                'vol_change': vol_change,
                'ma5': ma5,
                'ma20': ma20,
                'rsi': rsi,
                'atr': atr,
                'net_inflow': net_inflow,
            }

        except Exception as e:
            logger.debug(f"[ETF] {code} 指标计算失败: {e}")
            return None

    def _logic_etf_exit_check(self, row) -> str:
        """
        ETF 离场信号判定

        规则:
        - 破位 MA20 -> 清仓
        - RSI > 80 -> 减仓 (超买)
        - 大幅跌破 MA5 + 资金流出 -> 减仓
        - RSI < 25 -> 可加仓 (超卖)
        """
        close = row['close']
        ma20 = row['ma20']
        ma5 = row['ma5']
        rsi = row['rsi']
        net_inflow = row['net_inflow']
        change_pct = row['change_pct']

        # 趋势破位
        if close < ma20 * 0.98:  # 跌破 MA20 超过 2%
            return "🚨 [清仓] 趋势破位"

        # 超买信号
        if rsi > 80:
            return "💰 [减仓] RSI 超买"

        # 短线弱势 + 资金流出
        if close < ma5 and net_inflow < -1e7:  # 净流出超过1000万
            return "📉 [减仓] 短线弱势+资金流出"

        # 超跌反弹机会
        if rsi < 25 and net_inflow > 0:
            return "🟢 [加仓] 超跌反弹机会"

        # 轻微回调
        if close < ma5 and change_pct < -1:
            return "⏳ [观察] 短线回调"

        # 趋势良好
        if close > ma20 and close > ma5:
            return "✅ [持有] 趋势稳健"

        return "📊 [持有] 震荡整理"

    def _display_etf_report(self, df: pd.DataFrame) -> str:
        """展示 ETF 诊断报告并返回报告文本"""
        from ..notify import send_feishu_msg
        from datetime import datetime

        report_lines = []

        # 添加时间信息
        now = datetime.now()
        time_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # 判断市场状态
        hour = now.hour
        if hour < 9 or (hour == 9 and now.minute < 30):
            market_phase = "盘前"
        elif hour < 11 or (hour == 11 and now.minute < 30):
            market_phase = "上午盘中(成交数据不完整)"
        elif hour < 13:
            market_phase = "午间休市(成交为上午数据)"
        elif hour < 15:
            market_phase = "下午盘中" if hour < 14 or now.minute < 30 else "尾盘(数据接近完整)"
        else:
            market_phase = "盘后(全天完整数据)"

        # 头部
        header = f"ETF 持仓监控报告 | 时间: {time_str} | {market_phase} | 成交额: {self.market_total_amount/1e8:.0f}亿"
        report_lines.append(header)
        report_lines.append("-" * 120)

        # 表头
        report_lines.append(
            f"{self._pad_str('代码', 8)} {self._pad_str('名称', 12)} {self._pad_str('现价', 8)} "
            f"{self._pad_str('涨幅%', 8)} {self._pad_str('成交额(亿)', 10)} {self._pad_str('量比', 6)} "
            f"{self._pad_str('量能变化%', 10)} {self._pad_str('资金流(万)', 12)} {self._pad_str('RSI', 6)} "
            f"{self._pad_str('MA位置', 10)} {'策略建议'}"
        )
        report_lines.append("-" * 120)

        for _, r in df.iterrows():
            # MA位置
            if r['close'] > r['ma20']:
                ma_pos = "趋势上✅"
            elif r['close'] > r['ma5']:
                ma_pos = "MA5上⚠️"
            else:
                ma_pos = "破位🚨"

            # 资金流动方向
            net_inflow_wan = r['net_inflow'] / 1e4  # 转换为万

            line = (
                f"{self._pad_str(str(r['code']), 8)} {self._pad_str(str(r['name']), 12)} "
                f"{r['close']:<8.3f} {r['change_pct']:<8.2f} "
                f"{r['amount']/1e8:<10.2f} {r['vol_ratio']:<6.2f} {r['vol_change']*100:<10.1f} "
                f"{net_inflow_wan:<12.1f} {r['rsi']:<6.1f} {self._pad_str(ma_pos, 10)} {r['action']}"
            )
            report_lines.append(line)

        report_lines.append("=" * 120)

        report_text = "\n".join(report_lines)

        # 控制台输出
        print("\n" + "📈" * 15 + " ETF 持仓监控报告 " + "📈" * 15)
        for line in report_lines:
            print(line)
        print("█" * 120)

        # 飞书通知
        send_feishu_msg("ETF诊断报告", report_text)

        return report_text

    # ==================== ETF 行业轮动模块 ====================

    def _classify_etf_type(self, name: str) -> str:
        """
        根据 ETF 名称分类类型

        Args:
            name: ETF 名称

        Returns:
            类型: '行业', '宽基', '商品', '跨境', '债券', '货币', '其他'
        """
        from .. import config as cfg

        for etf_type, keywords in cfg.ETF_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in name:
                    return etf_type
        return '其他'

    def _calculate_etf_rotation_alpha(self, code: str, name: str, realtime_row: pd.Series,
                                       index_change_20d: float = 0) -> Optional[dict]:
        """
        计算 ETF 行业轮动 Alpha 因子

        Args:
            code: ETF 代码
            name: ETF 名称
            realtime_row: 实时行情数据
            index_change_20d: 沪深300 20日涨幅 (用于计算相对强弱)

        Returns:
            因子字典，失败返回 None
        """
        from .. import config as cfg
        from ..data import load_or_fetch_kline, fetch_kline_from_api

        try:
            # 1. 解析实时数据
            close = float(realtime_row.get('最新价', 0))
            change_pct = float(realtime_row.get('涨跌幅', 0))
            amount = float(realtime_row.get('成交额', 0))
            vol_ratio = float(realtime_row.get('量比', 0)) if '量比' in realtime_row else 1.0

            if amount < cfg.ETF_MIN_AMOUNT:
                return None

            # 2. 获取 K 线数据
            df = load_or_fetch_kline(code, fetch_kline_from_api, cfg.KLINE_START_DATE)

            if df is None or len(df) < 25:
                return None

            df = df.copy()
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
            df = df.sort_values('trade_date').reset_index(drop=True)

            # 3. 计算趋势动量 (20日涨幅)
            if len(df) >= 20:
                close_20d_ago = df['close'].iloc[-20]
                trend_momentum = (close - close_20d_ago) / close_20d_ago if close_20d_ago > 0 else 0
            else:
                trend_momentum = 0

            # 4. 计算 MA 位置
            ma5 = df['close'].iloc[-5:].mean() if len(df) >= 5 else close
            ma20 = df['close'].iloc[-20:].mean() if len(df) >= 20 else close
            ma60 = df['close'].iloc[-60:].mean() if len(df) >= 60 else close

            # MA 位置得分 (0-1)
            ma_score = 0
            if close > ma60:
                ma_score += 0.4
            if close > ma20:
                ma_score += 0.35
            if close > ma5:
                ma_score += 0.25

            # 5. 计算 RSI
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / (loss + 1e-9)
            rsi_series = 100 - (100 / (1 + rs))
            rsi = rsi_series.iloc[-1] if not rsi_series.empty else 50

            # RSI 惩罚/加分
            if rsi > cfg.ETF_RSI_UPPER:
                rsi_factor = 0.7  # 超买惩罚
            elif rsi < cfg.ETF_RSI_LOWER:
                rsi_factor = 1.2  # 超卖加分
            else:
                rsi_factor = 1.0

            # 6. 计算波动率
            returns = df['close'].pct_change().dropna()
            volatility = returns.iloc[-20:].std() if len(returns) >= 20 else 0.02

            # 风险调整收益 (20日收益/波动率)
            risk_adjusted = trend_momentum / (volatility + 0.01)

            # 7. 相对强弱 (vs 沪深300)
            relative_strength = trend_momentum - index_change_20d

            # 8. 资金流强度 (efinance, 区分盘中/盘后)
            fund_flow = 0
            try:
                from ..data import is_trading_hours
                import efinance as ef
                
                if is_trading_hours():
                    # 盘中: 获取今日实时资金流
                    df_bill = ef.stock.get_today_bill(code)
                    if df_bill is not None and not df_bill.empty:
                        last_row = df_bill.iloc[-1]
                        net_inflow = float(last_row.get('主力净流入', 0))
                        fund_flow = net_inflow / (amount + 1e-9)
                else:
                    # 盘后: 获取历史资金流最后一条
                    df_hist_bill = ef.stock.get_history_bill(code)
                    if df_hist_bill is not None and not df_hist_bill.empty:
                        last_row = df_hist_bill.iloc[-1]
                        net_inflow = float(last_row.get('主力净流入', 0))
                        fund_flow = net_inflow / (amount + 1e-9)
            except Exception:
                pass

            # 9. Alpha 合成
            weights = cfg.ETF_FACTOR_WEIGHTS
            alpha_score = (
                trend_momentum * weights['trend_momentum'] +
                fund_flow * weights['fund_flow'] * 10 +  # 放大系数
                relative_strength * weights['relative_strength'] +
                risk_adjusted * weights['risk_adjusted'] * 0.1 +  # 缩小系数
                ma_score * weights['ma_position']
            ) * rsi_factor

            return {
                'code': code,
                'name': name,
                'etf_type': self._classify_etf_type(name),
                'close': close,
                'change_pct': change_pct,
                'amount': amount,
                'trend_momentum': trend_momentum * 100,  # 转为百分比
                'relative_strength': relative_strength * 100,
                'fund_flow': fund_flow * 100,
                'risk_adjusted': risk_adjusted,
                'ma_score': ma_score,
                'rsi': rsi,
                'vol_ratio': vol_ratio,
                'ma5': ma5,
                'ma20': ma20,
                'alpha_score': alpha_score,
            }

        except Exception as e:
            logger.debug(f"[ETF Rotation] {code} 因子计算失败: {e}")
            return None

    def run_etf_market_scan(self) -> tuple:
        """
        ETF 全市场扫描 - 行业轮动策略

        Returns:
            (DataFrame, 报告文本)
        """
        from .. import config as cfg
        from ..data import fetch_etf_quotes_with_fallback, fetch_market_index
        from ..notify import send_feishu_msg
        from concurrent.futures import ThreadPoolExecutor, as_completed

        start_t = time.time()

        logger.info("=" * 60)
        logger.info("[ETF Scan] ETF 全市场扫描 - 行业轮动策略")
        logger.info("=" * 60)

        # 1. 获取沪深300 20日涨幅作为基准
        index_change_20d = 0
        try:
            df_index = fetch_market_index('000300')
            if df_index is not None and len(df_index) >= 20:
                df_index = df_index.sort_values('trade_date').reset_index(drop=True)
                df_index['close'] = pd.to_numeric(df_index['close'], errors='coerce')
                close_now = df_index['close'].iloc[-1]
                close_20d = df_index['close'].iloc[-20]
                index_change_20d = (close_now - close_20d) / close_20d if close_20d > 0 else 0
            logger.info(f"[基准] 沪深300 20日涨幅: {index_change_20d*100:.2f}%")
        except Exception as e:
            logger.warning(f"获取沪深300基准失败: {e}")

        # 2. 获取全市场 ETF 行情 (支持盘后回退)
        df_etf = fetch_etf_quotes_with_fallback(fs='ETF')
        if df_etf is None or df_etf.empty:
            logger.error("获取 ETF 行情失败")
            return pd.DataFrame(), ""

        logger.info(f"[ETF] 全市场 ETF 数量: {len(df_etf)}")

        # 3. 预筛选 (成交额)
        df_etf['成交额'] = pd.to_numeric(df_etf['成交额'], errors='coerce').fillna(0)
        df_etf['涨跌幅'] = pd.to_numeric(df_etf['涨跌幅'], errors='coerce').fillna(0)
        
        # 计算 ETF 市场总成交额
        self.market_total_amount = df_etf['成交额'].sum()
        
        df_candidates = df_etf[df_etf['成交额'] >= cfg.ETF_MIN_AMOUNT].copy()

        logger.info(f"[ETF] 成交额筛选后: {len(df_candidates)} 只，市场总成交额: {self.market_total_amount/1e8:.0f}亿")

        # 4. 计算因子
        results = []
        task_list = df_candidates[['股票代码', '股票名称']].values.tolist()

        with ThreadPoolExecutor(max_workers=self.max_io_workers) as executor:
            futures = {}
            for code, name in task_list:
                row = df_candidates[df_candidates['股票代码'] == code].iloc[0]
                futures[executor.submit(
                    self._calculate_etf_rotation_alpha,
                    code, name, row, index_change_20d
                )] = code

            for f in tqdm(as_completed(futures), total=len(task_list), desc="ETF因子计算"):
                res = f.result()
                if res:
                    results.append(res)

        if not results:
            logger.warning("[ETF] 无有效候选")
            return pd.DataFrame(), ""

        df_result = pd.DataFrame(results)
        logger.info(f"[ETF] 有效候选: {len(df_result)} 只")

        # 5. 分类选股
        final_picks = []
        type_counts = {}

        # 按 Alpha 排序
        df_sorted = df_result.sort_values('alpha_score', ascending=False)
        
        # 调试: 打印所有候选
        logger.info(f"[ETF Debug] 全部候选 ({len(df_sorted)} 只):")
        for _, row in df_sorted.iterrows():
            logger.info(f"  {row['code']} {row['name']} | 类型:{row['etf_type']} | Alpha:{row['alpha_score']:.3f} | RSI:{row['rsi']:.1f} | MA得分:{row['ma_score']:.2f}")

        # 优先选取: 行业 > 商品 > 跨境 > 宽基
        priority_types = ['行业', '商品', '跨境', '宽基']

        for etf_type in priority_types:
            df_type = df_sorted[df_sorted['etf_type'] == etf_type]
            for _, row in df_type.iterrows():
                if len(final_picks) >= cfg.ETF_MAX_TOTAL_PICKS:
                    logger.debug(f"[ETF Debug] 已达最大推荐数 {cfg.ETF_MAX_TOTAL_PICKS}")
                    break
                if type_counts.get(etf_type, 0) >= cfg.ETF_MAX_PICKS_PER_TYPE:
                    logger.debug(f"[ETF Debug] {row['code']} 跳过: 类型 {etf_type} 已达上限")
                    continue
                # 过滤条件
                if row['rsi'] > 80:  # 极度超买跳过
                    logger.info(f"[ETF Debug] {row['code']} {row['name']} 被过滤: RSI超买 ({row['rsi']:.1f} > 80)")
                    continue
                if row['ma_score'] < 0.5:  # MA 位置不佳跳过
                    logger.info(f"[ETF Debug] {row['code']} {row['name']} 被过滤: MA位置不佳 ({row['ma_score']:.2f} < 0.5)")
                    continue

                # 添加操作建议
                logger.info(f"[ETF Debug] {row['code']} {row['name']} ✅ 通过筛选")
                row_dict = row.to_dict()
                row_dict['action'] = self._get_etf_action_label(row)
                final_picks.append(row_dict)
                type_counts[etf_type] = type_counts.get(etf_type, 0) + 1

        df_picks = pd.DataFrame(final_picks)

        # 6. 输出报告
        report_text = self._display_etf_scan_report(df_picks, start_t, index_change_20d)

        return df_picks, report_text

    def _get_etf_action_label(self, row) -> str:
        """获取 ETF 操作建议标签"""
        alpha = row['alpha_score']
        trend = row['trend_momentum']
        relative = row['relative_strength']
        ma_score = row['ma_score']

        if alpha > 0.15 and trend > 5 and relative > 0 and ma_score >= 0.75:
            return "⚡ [强力买入]"
        elif alpha > 0.10 and trend > 3 and ma_score >= 0.6:
            return "🥈 [建议买入]"
        elif alpha > 0.05 and ma_score >= 0.5:
            return "📈 [可以关注]"
        else:
            return "📊 [观察]"

    def _display_etf_scan_report(self, df: pd.DataFrame, start_t: float, index_change: float) -> str:
        """展示 ETF 扫描报告"""
        from ..notify import send_feishu_msg

        report_lines = []

        # 头部
        header = f"ETF 行业轮动扫描 | 沪深300基准: {index_change*100:+.2f}% | 成交额: {self.market_total_amount/1e8:.0f}亿"
        report_lines.append(header)
        report_lines.append("=" * 130)

        if df.empty:
            report_lines.append("今日无推荐 ETF")
        else:
            # 表头
            report_lines.append(
                f"{'代码':<8} {'名称':<14} {'类型':<6} {'Alpha':<8} {'20D动量%':<10} "
                f"{'相对强弱%':<10} {'资金流%':<10} {'MA位置':<8} {'RSI':<6} {'涨幅%':<8} {'操作建议'}"
            )
            report_lines.append("-" * 130)

            for _, r in df.iterrows():
                # MA 位置显示
                if r['ma_score'] >= 0.75:
                    ma_display = "强势↑"
                elif r['ma_score'] >= 0.5:
                    ma_display = "健康→"
                else:
                    ma_display = "弱势↓"

                line = (
                    f"{r['code']:<8} {r['name']:<14} {r['etf_type']:<6} {r['alpha_score']:<8.3f} "
                    f"{r['trend_momentum']:<10.2f} {r['relative_strength']:<10.2f} "
                    f"{r['fund_flow']:<10.2f} {ma_display:<8} {r['rsi']:<6.1f} "
                    f"{r['change_pct']:<8.2f} {r['action']}"
                )
                report_lines.append(line)

        report_lines.append("=" * 130)
        report_lines.append(f"[耗时]: {time.time() - start_t:.1f}s")

        report_text = "\n".join(report_lines)

        # 控制台输出
        print("\n" + "🔄" * 15 + " ETF 行业轮动扫描 " + "🔄" * 15)
        for line in report_lines:
            print(line)
        print("█" * 130)

        # 飞书通知
        send_feishu_msg("ETF行业轮动扫描", report_text)

        return report_text

    def get_market_context(self) -> str:
        """
        生成市场情绪与资金流向分析报告

        Returns:
            市场上下文报告文本
        """
        from ..factors import get_connect_sentiment
        from ..factors.market import get_market_trend_state

        lines = []
        lines.append("=" * 60)
        lines.append("【市场情绪与资金流向分析】")
        lines.append("=" * 60)

        # 市场趋势判断 (ADX/ATR)
        trend_info = get_market_trend_state('000300')
        if trend_info:
            lines.append(f"• 市场状态: {trend_info.get('state_cn', '未知')} [ADX: {trend_info.get('adx', 0):.1f}]")
        
        # 市场成交额
        if self.market_total_amount > 0:
            amount_str = f"{self.market_total_amount / 1e8:.0f}亿"
            if self.market_total_amount < 8000e8:
                amount_level = "缩量市（谨慎）"
            elif self.market_total_amount > 20000e8:
                amount_level = "爆量市（警惕）"
            else:
                amount_level = "正常量能"
            lines.append(f"• 市场成交额: {amount_str} [{amount_level}]")

        # 市场宽度
        if self.market_breadth > 0:
            lines.append(f"• 市场宽度: {self.market_breadth:.1%} (涨跌比)")

        # 连板情绪
        emotion_desc = {
            'HOT': '🔥 情绪过热（建议减仓）',
            'COLD': '❄️ 情绪冰点（可加仓）',
            'NORMAL': '😐 情绪正常'
        }
        lines.append(f"• 连板情绪: {emotion_desc.get(self.streak_emotion, '未知')}")

        # 外资情绪
        connect_desc = {
            'INFLOW': '📈 外资积极看多',
            'OUTFLOW': '📉 外资谨慎撤离',
            'NEUTRAL': '➡️ 外资多空平衡'
        }
        lines.append(f"• 互联互通情绪: {connect_desc.get(self.connect_trend, '未知')}")

        # 仓位系数
        lines.append(f"• 策略仓位系数: {self.position_multiplier:.2f}x")

        lines.append("")
        lines.append("【港股通板块分析】")
        lines.append("-" * 40)

        # 获取港股通热门板块数据
        try:
            from ..data import fetch_realtime_quotes

            df_sh = fetch_realtime_quotes(fs='沪股通')
            df_sz = fetch_realtime_quotes(fs='深股通')

            frames = []
            if df_sh is not None and not df_sh.empty:
                frames.append(df_sh)
            if df_sz is not None and not df_sz.empty:
                frames.append(df_sz)

            if frames:
                df = pd.concat(frames, ignore_index=True)
                df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
                df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)

                # 成交额Top5
                top5 = df.nlargest(5, '成交额')[['股票代码', '股票名称', '涨跌幅', '成交额']]
                lines.append("• 港股通成交额Top5:")
                for _, row in top5.iterrows():
                    pct = row['涨跌幅']
                    arrow = "↑" if pct > 0 else "↓" if pct < 0 else "→"
                    lines.append(f"  - {row['股票名称']} ({row['股票代码']}): {arrow}{abs(pct):.2f}%")

                # 涨幅Top3 & 跌幅Top3
                up_top3 = df.nlargest(3, '涨跌幅')[['股票名称', '涨跌幅']]
                down_top3 = df.nsmallest(3, '涨跌幅')[['股票名称', '涨跌幅']]

                lines.append("• 港股通涨幅Top3:")
                for _, row in up_top3.iterrows():
                    lines.append(f"  - {row['股票名称']}: +{row['涨跌幅']:.2f}%")

                lines.append("• 港股通跌幅Top3:")
                for _, row in down_top3.iterrows():
                    lines.append(f"  - {row['股票名称']}: {row['涨跌幅']:.2f}%")

        except Exception as e:
            lines.append(f"• 港股通数据获取失败: {e}")

        lines.append("=" * 60)

        return "\n".join(lines)

    def get_gemini_advice(self, report_content: str, report_type: str) -> Optional[str]:
        """
        调用 Gemini API 获取 AI 交易建议（自动包含市场上下文）

        Args:
            report_content: 报告内容（选股或持仓诊断文本）
            report_type: 报告类型 ("尾盘选股" 或 "持仓诊断")

        Returns:
            AI 建议文本，失败返回 None
        """
        from ..notify import get_trading_advice, display_gemini_advice

        # 构建完整报告：市场上下文 + 原报告
        market_context = self.get_market_context()
        full_report = f"{market_context}\n\n{report_content}"

        advice = get_trading_advice(full_report, report_type)

        if advice:
            display_gemini_advice(advice, report_type)

        return advice

