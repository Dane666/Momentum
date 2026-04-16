# -*- coding: utf-8 -*-
"""
Ollama 本地大模型交互模块

提供 Ollama 本地大模型调用功能，用于量化策略分析和交易建议
"""

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger('momentum')


def get_market_time_context() -> dict:
    """
    获取当前市场时间上下文
    
    Returns:
        包含时间信息的字典
    """
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    
    # 判断市场状态
    if hour < 9 or (hour == 9 and minute < 30):
        market_status = "盘前"
        trading_phase = "开盘前，建议等待开盘后观察走势再操作"
    elif (hour == 9 and minute >= 30) or (hour == 10) or (hour == 11 and minute < 30):
        market_status = "上午盘中"
        trading_phase = "上午交易时段，成交量仅反映半日情况"
    elif hour == 11 and minute >= 30:
        market_status = "午间休市"
        trading_phase = "午间休市，成交数据为上午数据"
    elif hour >= 13 and hour < 15:
        market_status = "下午盘中"
        if hour == 14 and minute >= 30:
            trading_phase = "尾盘阶段，成交数据基本完整，适合做出操作决策"
        else:
            trading_phase = "下午交易时段，成交数据仍在累积"
    else:
        market_status = "盘后"
        trading_phase = "收盘后，成交数据为全天完整数据"
    
    # 判断星期几
    weekday = now.weekday()
    weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    weekday_name = weekday_names[weekday]
    
    return {
        'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H:%M:%S'),
        'weekday': weekday_name,
        'market_status': market_status,
        'trading_phase': trading_phase,
        'is_trading_day': weekday < 5,
    }


def build_ollama_prompt(report_content: str, time_context: dict = None) -> str:
    """
    构建 Ollama 分析提示词
    
    Args:
        report_content: 策略报告内容
        time_context: 时间上下文信息
        
    Returns:
        完整的提示词
    """
    if time_context is None:
        time_context = get_market_time_context()
    
    prompt = f"""你是一名资深 A 股量化交易专家，精通动量策略和技术分析。请基于以下完整的策略报告，给出专业的交易建议。

【重要时间信息】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 报告生成时间: {time_context['timestamp']}
- 当前日期: {time_context['date']} ({time_context['weekday']})
- 市场状态: {time_context['market_status']}
- 时段说明: {time_context['trading_phase']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ 注意: 如果当前为开盘初期或上午时段，成交量和成交额数据仅反映部分交易时段，
   请在分析时考虑这一因素，不要将部分时段数据当作全天数据进行判断。

【策略版本】: Momentum v16 工程化版本

===== 完整策略报告 =====
{report_content}
========================

【分析任务】:
请从以下维度进行深度分析：

1. 【市场环境研判】
   - 根据市场宽度、成交额、情绪指标判断当前市场状态（牛市/震荡/熊市）
   - 评估当前是否适合操作，给出仓位建议（满仓/半仓/轻仓/空仓）
   - ⚠️ 注意当前时间段，成交额可能仅为部分数据

2. 【持仓诊断建议】
   - 对每只持仓股票给出明确建议：持有/减仓/清仓
   - 说明理由（结合 RSI、MA、Alpha 等指标）
   - 给出具体止损位和止盈位

3. 【ETF 持仓建议】
   - 分析 ETF 持仓的行业配置是否合理
   - 是否需要调仓，给出具体建议

4. 【选股推荐分析】
   - 对推荐的候选股票进行优先级排序
   - 分析每只股票的买入时机、目标仓位
   - 给出入场价位区间和止损位

5. 【综合操作建议】
   - 结合当前时间 ({time_context['market_status']})，给出具体操作时机建议
   - 如果是盘中，建议等待还是立即操作
   - 如果是盘后，给出明日开盘的操作计划
   - 风险提示和注意事项

【输出要求】:
- 使用中文回答，结构清晰
- 建议具体可执行，避免模糊表述
- 关键数据用【】标注
- 风险提示用 ⚠️ 标注
/no_think"""
    
    return prompt


def get_ollama_trading_advice(
    report_content: str, 
    model: str = "qwen3:14b",
    send_notification: bool = True
) -> str:
    """
    调用 Ollama 本地大模型分析完整报告并给出选股建议

    Args:
        report_content: 完整的策略报告内容 (持仓诊断 + ETF诊断 + 选股结果)
        model: Ollama 模型名称，默认 qwen3:14b
        send_notification: 是否发送飞书通知

    Returns:
        AI 分析建议文本
    """
    import ollama
    from .feishu import send_feishu_msg
    
    # 获取时间上下文
    time_context = get_market_time_context()
    
    # 构建提示词
    prompt = build_ollama_prompt(report_content, time_context)

    try:
        logger.info(f"[Ollama] 正在调用 {model} 进行综合分析...")
        logger.info(f"[Ollama] 当前时间: {time_context['timestamp']} ({time_context['market_status']})")
        start_time = datetime.now()

        response = ollama.chat(
            model=model,
            messages=[{'role': 'user', 'content': prompt}]
        )

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"[Ollama] 分析完成，耗时 {elapsed:.1f} 秒")

        advice = response['message']['content'].strip()

        # 打印分析结果
        print("\n" + "=" * 80)
        print(f"🤖 Ollama AI 综合分析建议 (模型: {model})")
        print(f"📅 分析时间: {time_context['timestamp']} | 市场状态: {time_context['market_status']}")
        print("=" * 80)
        print(advice)
        print("=" * 80 + "\n")

        # 发送飞书通知
        if send_notification:
            notification_title = f"🤖 Ollama AI 综合分析 ({model}) - {time_context['market_status']}"
            send_feishu_msg(notification_title, advice)

        return advice

    except Exception as e:
        logger.error(f"[Ollama] 调用失败: {e}")
        return f"Ollama 分析失败: {e}"


def get_quick_advice(report_content: str, model: str = "qwen2.5:3b") -> str:
    """
    快速获取简短建议（使用轻量模型）
    
    Args:
        report_content: 报告内容
        model: 轻量模型名称
        
    Returns:
        简短建议文本
    """
    import ollama
    
    time_context = get_market_time_context()
    
    prompt = f"""你是 A 股量化交易专家。请用 3-5 句话总结以下报告的核心建议：

【当前时间】: {time_context['timestamp']} ({time_context['market_status']})

{report_content}

要求：
1. 直接给出操作建议（买入/持有/卖出）
2. 指出最重要的1-2只股票
3. 给出仓位建议
/no_think"""

    try:
        response = ollama.chat(model=model, messages=[{'role': 'user', 'content': prompt}])
        return response['message']['content'].strip()
    except Exception as e:
        logger.error(f"[Ollama Quick] 调用失败: {e}")
        return f"快速分析失败: {e}"
