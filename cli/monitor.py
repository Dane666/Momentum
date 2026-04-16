# -*- coding: utf-8 -*-
"""
持仓诊断相关命令

包含:
- run_portfolio_monitor: 股票+ETF 持仓诊断
- run_market_scan: 市场扫描选股
- run_full_workflow: 完整流程
- run_etf_scan: ETF 行业轮动扫描
- run_full_workflow_with_ollama: 完整流程 + Ollama 分析
"""

import logging
from typing import Tuple, List, Dict, Union
import pandas as pd

logger = logging.getLogger('momentum')


def run_portfolio_monitor(
    holdings: Union[List[str], Dict[str, float]],
    etf_holdings: List[str],
    watchlist: List[str],
    enable_gemini: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """执行持仓诊断 (股票 + ETF)

    Args:
        holdings: 股票持仓 (列表或字典{代码: 买入价})
        etf_holdings: ETF 持仓列表
        watchlist: 观察清单
        enable_gemini: 是否启用 Gemini AI 建议
        
    Returns:
        (stock_result, etf_result) 元组
    """
    from momentum.core import MomentumEngine

    logger.info("=" * 60)
    logger.info("Momentum v16 - 持仓诊断模式")
    logger.info("=" * 60)

    # 提取持仓代码列表
    if isinstance(holdings, dict):
        holding_codes = list(holdings.keys())
        holding_costs = holdings
    else:
        holding_codes = holdings
        holding_costs = {}

    engine = MomentumEngine(watchlist=watchlist, holdings=holding_codes, holding_costs=holding_costs)

    # 执行股票持仓分析
    logger.info("\n[Scene 1] 股票持仓诊断")
    stock_result, stock_report = engine.portfolio_realtime_monitor()

    # 执行 ETF 持仓分析
    logger.info("\n[Scene 2] ETF 持仓诊断")
    etf_result, etf_report = engine.etf_realtime_monitor(etf_holdings=etf_holdings)

    # 合并报告
    combined_reports = []
    if not stock_result.empty:
        combined_reports.append(f"=== 股票持仓诊断 ===\n{stock_report}")
    if not etf_result.empty:
        combined_reports.append(f"=== ETF 持仓诊断 ===\n{etf_report}")

    # 获取 Gemini AI 建议
    if enable_gemini and combined_reports:
        full_report = "\n\n".join(combined_reports)
        engine.get_gemini_advice(full_report, "持仓诊断(股票+ETF)")

    return stock_result, etf_result


def run_market_scan(
    holdings: Union[List[str], Dict[str, float]],
    watchlist: List[str],
    enable_gemini: bool = False
) -> pd.DataFrame:
    """执行市场扫描选股

    Args:
        holdings: 股票持仓 (列表或字典{代码: 买入价})
        watchlist: 观察清单
        enable_gemini: 是否启用 Gemini AI 建议
        
    Returns:
        选股结果 DataFrame
    """
    from momentum.core import MomentumEngine

    logger.info("=" * 60)
    logger.info("Momentum v16 - 市场扫描模式")
    logger.info("=" * 60)

    # 提取持仓代码列表
    if isinstance(holdings, dict):
        holding_codes = list(holdings.keys())
    else:
        holding_codes = holdings

    engine = MomentumEngine(watchlist=watchlist, holdings=holding_codes)

    # 执行尾盘选股
    logger.info("\n[Scene] 尾盘选股")
    result, report_text = engine.run_all_market_scan_pro()

    # 获取 Gemini AI 建议
    if enable_gemini and not result.empty:
        engine.get_gemini_advice(report_text, "尾盘选股")

    return result


def run_full_workflow(
    holdings: Union[List[str], Dict[str, float]],
    etf_holdings: List[str],
    watchlist: List[str],
    enable_gemini: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """执行完整流程: 持仓诊断 + ETF诊断 + 市场扫描

    Args:
        holdings: 股票持仓 (列表或字典{代码: 买入价})
        etf_holdings: ETF 持仓列表
        watchlist: 观察清单
        enable_gemini: 是否启用 Gemini AI 建议
        
    Returns:
        (hold_result, etf_result, scan_result) 元组
    """
    from momentum.core import MomentumEngine

    logger.info("=" * 60)
    logger.info("Momentum v16 - 完整流程模式")
    logger.info("=" * 60)

    # 提取持仓代码列表
    if isinstance(holdings, dict):
        holding_codes = list(holdings.keys())
        holding_costs = holdings
    else:
        holding_codes = holdings
        holding_costs = {}

    engine = MomentumEngine(watchlist=watchlist, holdings=holding_codes, holding_costs=holding_costs)

    # 场景1: 股票持仓诊断
    logger.info("\n" + "=" * 60)
    logger.info("[Scene 1] 股票持仓诊断")
    logger.info("=" * 60)
    hold_result, hold_report = engine.portfolio_realtime_monitor()

    # 场景2: ETF持仓诊断
    logger.info("\n" + "=" * 60)
    logger.info("[Scene 2] ETF持仓诊断")
    logger.info("=" * 60)
    etf_result, etf_report = engine.etf_realtime_monitor(etf_holdings=etf_holdings)

    # 场景3: 尾盘选股
    logger.info("\n" + "=" * 60)
    logger.info("[Scene 3] 尾盘选股")
    logger.info("=" * 60)
    scan_result, scan_report = engine.run_all_market_scan_pro()

    # 获取 Gemini AI 建议
    if enable_gemini:
        combined_reports = []
        if not hold_result.empty:
            combined_reports.append(f"=== 股票持仓诊断 ===\n{hold_report}")
        if not etf_result.empty:
            combined_reports.append(f"=== ETF 持仓诊断 ===\n{etf_report}")
        if not scan_result.empty:
            combined_reports.append(f"=== 尾盘选股 ===\n{scan_report}")

        if combined_reports:
            full_report = "\n\n".join(combined_reports)
            engine.get_gemini_advice(full_report, "完整流程(股票+ETF+选股)")

    return hold_result, etf_result, scan_result


def run_etf_scan(
    holdings: List[str],
    watchlist: List[str],
    enable_gemini: bool = False
) -> pd.DataFrame:
    """执行 ETF 全市场扫描 - 行业轮动策略

    Args:
        holdings: 股票持仓列表
        watchlist: 观察清单
        enable_gemini: 是否启用 Gemini AI 建议
        
    Returns:
        ETF 扫描结果 DataFrame
    """
    from momentum.core import MomentumEngine

    logger.info("=" * 60)
    logger.info("Momentum v16 - ETF 行业轮动扫描模式")
    logger.info("=" * 60)

    engine = MomentumEngine(watchlist=watchlist, holdings=holdings)

    # 执行 ETF 全市场扫描
    logger.info("\n[Scene] ETF 行业轮动扫描")
    result, report_text = engine.run_etf_market_scan()

    # 获取 Gemini AI 建议
    if enable_gemini and not result.empty:
        engine.get_gemini_advice(report_text, "ETF行业轮动")

    return result


def run_full_workflow_with_ollama(
    holdings: Union[List[str], Dict[str, float]],
    etf_holdings: List[str],
    watchlist: List[str],
    enable_gemini: bool = False,
    ollama_model: str = "qwen3:14b"
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """执行完整流程并调用 Ollama 进行综合分析

    Args:
        holdings: 股票持仓 (列表或字典{代码: 买入价})
        etf_holdings: ETF 持仓列表
        watchlist: 观察清单
        enable_gemini: 是否启用 Gemini AI 建议
        ollama_model: Ollama 模型名称
        
    Returns:
        (hold_result, etf_result, scan_result) 元组
    """
    from momentum.core import MomentumEngine
    from momentum.notify import get_ollama_trading_advice

    logger.info("=" * 60)
    logger.info("Momentum v16 - 完整流程 + Ollama 分析模式")
    logger.info("=" * 60)

    # 提取持仓代码列表
    if isinstance(holdings, dict):
        holding_codes = list(holdings.keys())
        holding_costs = holdings
    else:
        holding_codes = holdings
        holding_costs = {}

    engine = MomentumEngine(watchlist=watchlist, holdings=holding_codes, holding_costs=holding_costs)

    # 收集所有报告
    all_reports = []

    # 场景1: 股票持仓诊断
    logger.info("\n" + "=" * 60)
    logger.info("[Scene 1] 股票持仓诊断")
    logger.info("=" * 60)
    hold_result, hold_report = engine.portfolio_realtime_monitor()
    if not hold_result.empty and hold_report:
        all_reports.append(f"=== 股票持仓诊断 ===\n{hold_report}")

    # 场景2: ETF持仓诊断
    logger.info("\n" + "=" * 60)
    logger.info("[Scene 2] ETF持仓诊断")
    logger.info("=" * 60)
    etf_result, etf_report = engine.etf_realtime_monitor(etf_holdings=etf_holdings)
    if not etf_result.empty and etf_report:
        all_reports.append(f"=== ETF 持仓诊断 ===\n{etf_report}")

    # 场景3: 尾盘选股
    logger.info("\n" + "=" * 60)
    logger.info("[Scene 3] 尾盘选股")
    logger.info("=" * 60)
    scan_result, scan_report = engine.run_all_market_scan_pro()
    if not scan_result.empty and scan_report:
        all_reports.append(f"=== 尾盘选股推荐 ===\n{scan_report}")

    # 合并报告并调用 Ollama 分析
    if all_reports:
        full_report = "\n\n".join(all_reports)

        # 添加市场上下文
        market_context = engine.get_market_context()
        full_report_with_context = f"{market_context}\n\n{full_report}"

        # 调用 Ollama 进行综合分析
        get_ollama_trading_advice(full_report_with_context, model=ollama_model)

        # 可选: 同时获取 Gemini 建议
        if enable_gemini:
            engine.get_gemini_advice(full_report_with_context, "完整流程(股票+ETF+选股)")

    return hold_result, etf_result, scan_result
