# -*- coding: utf-8 -*-
"""月度策略综述 — 把三份月度报告合并为一条 Bark 推送。

原 strategy-align-report 每月 1 日连推 3 条:
  1) align_report.py  --push   回测 vs 实盘对齐
  2) trade_log.py     --push   实盘交易日志
  3) trade_journal.py --factors --push  策略/因子有效性明细

三条打断多、口径散。本脚本聚合三份文本为一条「月度策略综述」,
统一标题 + 分节, 由 workflow 只推一条 Bark。

用法:
  python tools/monthly_report.py             # 打印 + 推送
  python tools/monthly_report.py --no-push   # 仅打印
"""
import argparse
import sys
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent
if str(PROJ) not in sys.path:
    sys.path.insert(0, str(PROJ))

from tools.align_report import build_report
from tools.trade_log import collect, summarize
from tools.trade_journal import evaluate_data, _format

TITLE = "📆 月度策略综述"


def build_monthly_report():
    """拼装月度综述文本。返回 (title, body)。"""
    sections = []

    # 1. 回测 vs 实盘对齐
    try:
        align_text, _bt, _live = build_report()
        sections.append(("📐 回测 vs 实盘对齐", align_text))
    except Exception as e:
        sections.append(("📐 回测 vs 实盘对齐", f"(生成失败: {e})"))

    # 2. 实盘交易日志
    try:
        realized, open_, watching = collect()
        log_text = summarize(realized, open_, watching)
        sections.append(("📒 实盘交易日志", log_text))
    except Exception as e:
        sections.append(("📒 实盘交易日志", f"(生成失败: {e})"))

    # 3. 策略/因子有效性 (trade_journal._format 已含按 type 分组的 1d/3d/5d/10d/突破跟随/已触发明细)
    try:
        sim_agg, real_agg, sim_total, real_total = evaluate_data()
        factor_text = _format(sim_agg, real_agg, None, sim_total, real_total)
        sections.append(("🧬 信号质量 / 因子有效性", factor_text))
    except Exception as e:
        sections.append(("🧬 信号质量 / 因子有效性", f"(生成失败: {e})"))

    # 拼装: 每个 section 前加分隔 + 小标题
    parts = []
    for i, (subtitle, body) in enumerate(sections):
        parts.append("─" * 40)
        parts.append(subtitle)
        parts.append("─" * 40)
        parts.append(body)
        if i < len(sections) - 1:
            parts.append("")
    return TITLE, "\n".join(parts)


def main():
    ap = argparse.ArgumentParser(description="月度策略综述(合并 3 条月度 Bark 为 1 条)")
    ap.add_argument('--no-push', action='store_true', help="仅打印不推送")
    a = ap.parse_args()

    title, body = build_monthly_report()
    print(body)

    if not a.no_push:
        try:
            from tools.tracking_utils import bark_notify
            bark_notify(title, body)
            print("[monthly] Bark 推送成功:", title)
        except Exception as e:
            print("[monthly] Bark 推送失败:", e)


if __name__ == '__main__':
    main()
