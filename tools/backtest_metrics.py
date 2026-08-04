# -*- coding: utf-8 -*-
"""快照式回测对齐口径(评估报告建议⑤) — 统一指标定义。

问题: scan(live 信号评估) 与 backtest(前向验证) 过去各自算收益/胜率/夏普,
定义不完全一致, 导致"样本内/外"数字不可直接比较。

本模块把指标定义收敛为单一真相源, 供 trade_journal(实盘/模拟评估)、
align_report(对齐报告)、以及(可选)backtest 共用, 保证口径一致。

口径约定(CONVENTIONS):
  - 单笔收益 ret: 卖出价/买入价 - 1 (已含滑点/费由各自上游负责, 本层只算统计)
  - 总收益 total_ret: ∏(1+ret) - 1  (复利累计, 与 harness.metrics 一致)
  - 胜率 win_rate: ret>0 占比
  - 平均每笔 avg_ret: mean(ret)
  - 最大回撤 max_dd: 由累计权益曲线取峰后最大回落
  - 夏普 sharpe: 对"逐笔收益序列"做 mean/std * sqrt(252) 的年化近似
      (backtest 侧用日频权益曲线; live 侧用逐笔序列, 二者均为年化近似, 已在报告中标注)
  - 年化因子 ANNUALIZATION = 252
"""
import math
from collections import defaultdict

ANNUALIZATION = 252

CONVENTIONS = {
    "total_ret": "∏(1+ret) - 1 (复利累计)",
    "win_rate": "ret>0 占比",
    "avg_ret": "mean(ret) 平均每笔",
    "max_dd": "累计权益曲线峰后最大回落",
    "sharpe": "mean(ret)/std(ret) * sqrt(252) 年化近似",
    "annualization": ANNUALIZATION,
    "fee_note": "滑点/费由各自上游(scan/backtest)负责, 本层只做统计聚合",
}


def _as_float(x):
    try:
        return float(x)
    except Exception:
        return None


def equity_curve(returns, start=1.0):
    """由逐笔收益序列生成累计权益曲线(list[float]), 起点 start。"""
    eq = [start]
    for r in returns:
        rf = _as_float(r)
        if rf is None:
            eq.append(eq[-1])
        else:
            eq.append(eq[-1] * (1.0 + rf))
    return eq


def max_drawdown(returns):
    """由逐笔收益序列算最大回撤(正值百分比, 0.12 = 回撤12%)。"""
    eq = equity_curve(returns)
    peak = eq[0]
    mdd = 0.0
    for v in eq:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd
    return mdd


def sharpe(returns, annualization=ANNUALIZATION):
    """逐笔收益序列的夏普年化近似。样本<2 或 std=0 返回 0.0。"""
    rs = [_as_float(r) for r in returns if _as_float(r) is not None]
    if len(rs) < 2:
        return 0.0
    mean = sum(rs) / len(rs)
    var = sum((x - mean) ** 2 for x in rs) / (len(rs) - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return 0.0
    return mean / sd * math.sqrt(annualization)


def per_trade_metrics(returns):
    """对一组逐笔收益(小数)返回统一口径指标 dict。

    Returns:
        dict(n, total_ret, win_rate, avg_ret, max_dd, sharpe)
        全部以"百分比友好"保留: total_ret/avg_ret 为小数; 调用方自行 *100。
    """
    rs = [_as_float(r) for r in returns if _as_float(r) is not None]
    n = len(rs)
    if n == 0:
        return dict(n=0, total_ret=0.0, win_rate=0.0, avg_ret=0.0, max_dd=0.0, sharpe=0.0)
    total = 1.0
    for r in rs:
        total *= (1.0 + r)
    total_ret = total - 1.0
    wins = sum(1 for r in rs if r > 0)
    win_rate = wins / n
    avg_ret = sum(rs) / n
    return dict(n=n, total_ret=total_ret, win_rate=win_rate,
                avg_ret=avg_ret, max_dd=max_drawdown(rs), sharpe=sharpe(rs))


def summarize(text_mode=True, agg=None):
    """返回口径说明字符串(供报告附录)。agg: 可选已算指标, 用于示例。"""
    lines = ["📐 统一指标口径(CONVENTIONS):"]
    for k, v in CONVENTIONS.items():
        lines.append(f"  - {k}: {v}")
    return "\n".join(lines)


# 便捷: 把 fraction 列表直接算并显示为百分比的 helper
def fmt_pct(x):
    return f"{x*100:+.1f}%"


if __name__ == '__main__':
    demo = [0.10, -0.05, 0.20, 0.03, -0.02, 0.15]
    m = per_trade_metrics(demo)
    print("demo metrics:", {k: (fmt_pct(v) if k in ('total_ret', 'avg_ret', 'win_rate', 'max_dd') else round(v, 3)) for k, v in m.items()})
    print(summarize())
