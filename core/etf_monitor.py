# -*- coding: utf-8 -*-
"""
ETF Monitor - ETF 诊断与轮动模块

职责:
- ETF 实时持仓诊断
- ETF 市场扫描轮动
- ETF 因子计算

从 engine.py 提取，遵循单一职责原则。
"""

import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Optional, Tuple
import logging

logger = logging.getLogger('momentum.etf')


class EtfMonitor:
    """
    ETF 监控与轮动扫描器
    
    功能:
    1. ETF 实时持仓诊断
    2. ETF 市场轮动扫描
    """
    
    def __init__(self, engine):
        """
        初始化 ETF 监控器
        
        Args:
            engine: MomentumEngine 实例
        """
        self.engine = engine
    
    def monitor(self, etf_holdings: list = None) -> Tuple[pd.DataFrame, str]:
        """
        ETF 持仓诊断
        
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

        df_etf = fetch_realtime_quotes(fs='ETF')
        if df_etf is None or df_etf.empty:
            logger.error("获取 ETF 实时行情失败")
            return pd.DataFrame(), ""

        df_etf['股票代码'] = df_etf['股票代码'].astype(str)
        df_hold = df_etf[df_etf['股票代码'].isin(etf_holdings)].copy()

        if df_hold.empty:
            logger.warning("[ETF] 持仓 ETF 不在行情数据中")
            return pd.DataFrame(), ""

        results = []
        for _, row in df_hold.iterrows():
            code = row['股票代码']
            name = row['股票名称']
            indicators = self._calculate_indicators(code, name, row, df_etf)
            if indicators:
                results.append(indicators)

        if not results:
            logger.warning("[ETF] 无法计算 ETF 因子")
            return pd.DataFrame(), ""

        df_result = pd.DataFrame(results)
        df_result['action'] = df_result.apply(self._exit_check, axis=1)

        report_text = self._display_report(df_result)

        return df_result, report_text
    
    def scan(self) -> Tuple[pd.DataFrame, str]:
        """
        ETF 市场轮动扫描
        
        Returns:
            (选股结果DataFrame, 报告文本)
        """
        from .. import config as cfg
        from ..data import fetch_realtime_quotes, fetch_market_index
        
        start_t = time.time()

        logger.info("=" * 60)
        logger.info("[ETF轮动] ETF 行业轮动扫描")
        logger.info("=" * 60)

        # 获取基准指数涨幅
        index_change = 0.0
        try:
            bench = fetch_market_index(index_code='000300', k_type=1)
            if bench is not None and len(bench) >= 2:
                index_change = ((bench['close'].iloc[-1] / bench['close'].iloc[-2]) - 1) * 100
        except Exception as e:
            logger.warning(f"获取基准指数失败: {e}")

        df_etf = fetch_realtime_quotes(fs='ETF')
        if df_etf is None or df_etf.empty:
            logger.error("获取 ETF 实时行情失败")
            return pd.DataFrame(), ""

        # 数据清洗
        df_etf['股票代码'] = df_etf['股票代码'].astype(str)
        df_etf['涨跌幅'] = pd.to_numeric(df_etf['涨跌幅'], errors='coerce').fillna(0)
        df_etf['成交额'] = pd.to_numeric(df_etf['成交额'], errors='coerce').fillna(0)
        df_etf['量比'] = pd.to_numeric(df_etf.get('量比', 0), errors='coerce').fillna(0)

        # 过滤成交额过低的 ETF
        min_amount = getattr(cfg, 'ETF_MIN_AMOUNT', 50000000)
        df_main = df_etf[df_etf['成交额'] >= min_amount].copy()

        logger.info(f"[ETF] 符合成交额要求的 ETF: {len(df_main)}")

        # 计算因子
        results = []
        for _, row in tqdm(df_main.iterrows(), total=len(df_main), desc="ETF因子计算"):
            code = row['股票代码']
            name = row['股票名称']
            indicators = self._calculate_rotation_alpha(code, name, row, df_etf)
            if indicators:
                results.append(indicators)

        if not results:
            return pd.DataFrame(), ""

        df_result = pd.DataFrame(results)

        # 分类型选股
        df_result = self._type_diversified_select(df_result, cfg)

        # 生成操作建议
        df_result['action'] = df_result.apply(self._get_action_label, axis=1)

        report_text = self._display_scan_report(df_result, start_t, index_change)

        return df_result, report_text
    
    def _calculate_indicators(
        self, 
        code: str, 
        name: str, 
        realtime_row: pd.Series, 
        df_all_etf: pd.DataFrame
    ) -> Optional[dict]:
        """计算 ETF 持仓诊断因子"""
        from .. import config as cfg
        from ..data import load_or_fetch_kline, fetch_kline_from_api

        try:
            close = float(realtime_row.get('最新价', 0))
            change_pct = float(realtime_row.get('涨跌幅', 0))
            amount = float(realtime_row.get('成交额', 0))
            vol_ratio = float(realtime_row.get('量比', 0)) if '量比' in realtime_row else 0

            df = load_or_fetch_kline(code, fetch_kline_from_api, cfg.KLINE_START_DATE)

            ma5, ma20, rsi, atr = 0, 0, 50, 0
            turnover_5d_avg = 0

            if df is not None and len(df) >= 20:
                df = df.copy()
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
                df = df.sort_values('trade_date').reset_index(drop=True)

                ma5 = df['close'].iloc[-5:].mean() if len(df) >= 5 else close
                ma20 = df['close'].iloc[-20:].mean() if len(df) >= 20 else close

                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / (loss + 1e-9)
                rsi_series = 100 - (100 / (1 + rs))
                rsi = rsi_series.iloc[-1] if not rsi_series.empty else 50

                turnover_5d_avg = df['amount'].iloc[-5:].mean() if len(df) >= 5 else amount

            # 成交额排名
            df_all_etf['成交额'] = pd.to_numeric(df_all_etf['成交额'], errors='coerce').fillna(0)
            total_etf = len(df_all_etf)
            amount_rank = (df_all_etf['成交额'] >= amount).sum()
            amount_rank_pct = amount_rank / total_etf if total_etf > 0 else 0.5

            # 资金净流入
            net_inflow = self._get_net_inflow(code)

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
    
    def _calculate_rotation_alpha(
        self, 
        code: str, 
        name: str, 
        realtime_row: pd.Series,
        df_all_etf: pd.DataFrame
    ) -> Optional[dict]:
        """计算 ETF 轮动 Alpha"""
        from .. import config as cfg
        from ..data import load_or_fetch_kline, fetch_kline_from_api

        try:
            close = float(realtime_row.get('最新价', 0))
            change_pct = float(realtime_row.get('涨跌幅', 0))
            amount = float(realtime_row.get('成交额', 0))
            vol_ratio = float(realtime_row.get('量比', 0)) if '量比' in realtime_row else 0

            df = load_or_fetch_kline(code, fetch_kline_from_api, cfg.KLINE_START_DATE)

            if df is None or len(df) < 20:
                return None

            df = df.copy()
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
            df = df.sort_values('trade_date').reset_index(drop=True)

            # 技术指标
            ma5 = df['close'].iloc[-5:].mean()
            ma20 = df['close'].iloc[-20:].mean()
            
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / (loss + 1e-9)
            rsi = 100 - (100 / (1 + rs.iloc[-1]))

            # 动量因子
            mom_5 = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100 if len(df) >= 6 else 0
            mom_20 = (df['close'].iloc[-1] / df['close'].iloc[-21] - 1) * 100 if len(df) >= 21 else 0

            # 资金流入
            net_inflow = self._get_net_inflow(code)

            # ETF 类型分类
            etf_type = self._classify_type(name)

            # Alpha 计算
            rsi_upper = getattr(cfg, 'ETF_RSI_UPPER', 75)
            rsi_lower = getattr(cfg, 'ETF_RSI_LOWER', 30)
            
            alpha = (
                mom_5 * 0.4 +
                mom_20 * 0.2 +
                (net_inflow / 1e8) * 0.3 +  # 资金流入 (亿元)
                vol_ratio * 0.1
            )
            
            # RSI 惩罚/奖励
            if rsi > rsi_upper:
                alpha *= 0.7  # 超买惩罚
            elif rsi < rsi_lower:
                alpha *= 1.2  # 超卖加分

            return {
                'code': code,
                'name': name,
                'etf_type': etf_type,
                'close': close,
                'change_pct': change_pct,
                'amount': amount,
                'vol_ratio': vol_ratio,
                'ma5': ma5,
                'ma20': ma20,
                'rsi': rsi,
                'mom_5': mom_5,
                'mom_20': mom_20,
                'net_inflow': net_inflow,
                'alpha': alpha,
            }

        except Exception as e:
            logger.debug(f"[ETF] {code} 轮动因子计算失败: {e}")
            return None
    
    def _get_net_inflow(self, code: str) -> float:
        """获取资金净流入"""
        try:
            from ..data import is_trading_hours
            import efinance as ef
            
            if is_trading_hours():
                df_bill = ef.stock.get_today_bill(code)
                if df_bill is not None and not df_bill.empty:
                    return float(df_bill.iloc[-1].get('主力净流入', 0))
            else:
                df_hist_bill = ef.stock.get_history_bill(code)
                if df_hist_bill is not None and not df_hist_bill.empty:
                    return float(df_hist_bill.iloc[-1].get('主力净流入', 0))
        except Exception:
            pass
        return 0
    
    def _classify_type(self, name: str) -> str:
        """分类 ETF 类型"""
        from .. import config as cfg
        
        keywords = getattr(cfg, 'ETF_TYPE_KEYWORDS', {})
        
        for etf_type, kw_list in keywords.items():
            for kw in kw_list:
                if kw in name:
                    return etf_type
        return '其他'
    
    def _type_diversified_select(self, df: pd.DataFrame, cfg) -> pd.DataFrame:
        """按类型分散选股"""
        max_per_type = getattr(cfg, 'ETF_MAX_PICKS_PER_TYPE', 2)
        max_total = getattr(cfg, 'ETF_MAX_TOTAL_PICKS', 5)
        
        final_picks = []
        type_counts = {}
        
        for _, row in df.sort_values('alpha', ascending=False).iterrows():
            if len(final_picks) >= max_total:
                break
            
            etf_type = row['etf_type']
            if type_counts.get(etf_type, 0) < max_per_type:
                final_picks.append(row.to_dict())
                type_counts[etf_type] = type_counts.get(etf_type, 0) + 1
        
        return pd.DataFrame(final_picks)
    
    def _exit_check(self, row) -> str:
        """ETF 离场信号判定"""
        close = row['close']
        ma20 = row['ma20']
        ma5 = row['ma5']
        rsi = row['rsi']
        net_inflow = row['net_inflow']
        change_pct = row['change_pct']

        if close < ma20 * 0.98:
            return "🚨 [清仓] 趋势破位"

        if rsi > 80:
            return "💰 [减仓] RSI 超买"

        if close < ma5 and net_inflow < -1e7:
            return "📉 [减仓] 短线弱势+资金流出"

        if rsi < 25 and net_inflow > 0:
            return "🟢 [加仓] 超跌反弹机会"

        if close < ma5 and change_pct < -1:
            return "⏳ [观察] 短线回调"

        if close > ma20 and close > ma5:
            return "✅ [持有] 趋势稳健"

        return "📊 [持有] 震荡整理"
    
    def _get_action_label(self, row) -> str:
        """轮动扫描操作建议"""
        close = row['close']
        ma20 = row['ma20']
        ma5 = row['ma5']
        rsi = row['rsi']
        alpha = row['alpha']
        
        if close > ma20 and close > ma5 and alpha > 0:
            return "🟢 [建仓] 趋势向好"
        
        if rsi > 75:
            return "⚠️ [观望] 短期超买"
        
        if close < ma20:
            return "🔴 [回避] 趋势向下"
        
        return "📊 [关注] 等待确认"
    
    def _display_report(self, df: pd.DataFrame) -> str:
        """展示 ETF 持仓诊断报告"""
        from ..notify import send_feishu_msg
        from datetime import datetime

        report_lines = []
        now = datetime.now()
        time_str = now.strftime('%Y-%m-%d %H:%M:%S')

        report_lines.append(f"📅 时间: {time_str}")
        report_lines.append("=" * 120)
        
        header = f"{'代码':<8} {'名称':<12} {'现价':<8} {'涨跌%':<8} {'RSI':<6} {'MA5':<8} {'MA20':<8} {'操作建议'}"
        report_lines.append(header)
        report_lines.append("-" * 120)

        for _, row in df.iterrows():
            line = (
                f"{row['code']:<8} {row['name']:<12} {row['close']:<8.2f} "
                f"{row['change_pct']:<8.2f} {row['rsi']:<6.1f} "
                f"{row['ma5']:<8.2f} {row['ma20']:<8.2f} {row['action']}"
            )
            report_lines.append(line)

        report_lines.append("=" * 120)
        
        report_text = "\n".join(report_lines)
        
        print("\n" + "📊" * 15 + " ETF 持仓诊断 " + "📊" * 15)
        for line in report_lines:
            print(line)
        
        send_feishu_msg("ETF持仓诊断", report_text)
        
        return report_text
    
    def _display_scan_report(self, df: pd.DataFrame, start_t: float, index_change: float) -> str:
        """展示 ETF 轮动扫描报告"""
        from ..notify import send_feishu_msg
        from datetime import datetime

        elapsed = time.time() - start_t
        now = datetime.now()
        time_str = now.strftime('%Y-%m-%d %H:%M:%S')

        report_lines = []
        report_lines.append(f"📅 时间: {time_str} | 耗时: {elapsed:.1f}s | 基准涨跌: {index_change:+.2f}%")
        report_lines.append("=" * 140)
        
        header = f"{'代码':<8} {'名称':<16} {'类型':<8} {'现价':<8} {'涨跌%':<8} {'Mom5':<8} {'RSI':<6} {'Alpha':<8} {'建议'}"
        report_lines.append(header)
        report_lines.append("-" * 140)

        for _, row in df.iterrows():
            line = (
                f"{row['code']:<8} {row['name']:<16} {row['etf_type']:<8} "
                f"{row['close']:<8.2f} {row['change_pct']:<8.2f} "
                f"{row['mom_5']:<8.2f} {row['rsi']:<6.1f} "
                f"{row['alpha']:<8.2f} {row['action']}"
            )
            report_lines.append(line)

        report_lines.append("=" * 140)
        
        report_text = "\n".join(report_lines)
        
        print("\n" + "🔄" * 15 + " ETF 轮动扫描 " + "🔄" * 15)
        for line in report_lines:
            print(line)
        
        send_feishu_msg("ETF轮动扫描", report_text)
        
        return report_text
