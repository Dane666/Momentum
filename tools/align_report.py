# -*- coding: utf-8 -*-
"""快照式回测 vs 实盘 对齐报告(评估报告建议⑤)。

目标: 把"样本内/外回测(forward_validation 等)"与"实盘/模拟信号评估(trade_journal)"放在
同一套指标口径(见 tools/backtest_metrics.CONVENTIONS)下并排比较, 暴露二者偏差,
避免"回测很美、实盘很骨感"的口径错位。

数据来源:
  - Backtest 侧: opt_study/*_metrics.json(含 total_ret/winrate/n/sharpe 的 metric 块)
  - Live 侧:     tools/trade_journal.evaluate_data() 的 sim_agg(按 type 分组的 10 日指标)

输出: 并排表 + 口径说明。可选 --export 写 HTML, --push 发 Bark。
"""
import sys, os, json, glob, argparse
from pathlib import Path
from collections import defaultdict

PROJ = Path(__file__).resolve().parent.parent
if str(PROJ) not in sys.path:
    sys.path.insert(0, str(PROJ))
OPT = PROJ / 'opt_study'

# 提取含标准 metric 字段的 dict(total_ret/winrate/n/sharpe)
_METRIC_KEYS = {'total_ret', 'winrate', 'n', 'sharpe'}


def _walk_metrics(obj, path, out):
    """递归收集所有像 backtest metric 的 dict(含 total_ret/winrate/n/sharpe)。

    列表不追加下标(保持父级 key 作为标签), 避免 'hold[0]' 这类噪音标签。
    """
    if isinstance(obj, dict):
        if _METRIC_KEYS.issubset(set(obj.keys())):
            out.append((path, dict(obj)))
        for k, v in obj.items():
            _walk_metrics(v, f"{path}/{k}", out)
    elif isinstance(obj, list):
        for v in obj:
            _walk_metrics(v, path, out)


def gather_backtest():
    """扫描 opt_study 下所有 *_metrics.json, 提取回测 metric 块(去重 + 限制数量)。"""
    raw = []
    for fp in sorted(glob.glob(str(OPT / '*_metrics.json'))):
        try:
            data = json.loads(Path(fp).read_text(encoding='utf-8'))
        except Exception:
            continue
        found = []
        _walk_metrics(data, Path(fp).stem, found)
        for path, m in found:
            raw.append((path, _nice_label(path), m))
    # 优先级: ① 规范汇总块(非 grid/sweep/tracker) > ② 已知策略标签 > ③ 样本量降序
    # 这样能让"样本内基线 / OOS 已发布组合"等关键结论排在前, 不被大量网格项刷屏。
    _NOISE = ('/grid', 'train_grid', '/sweep', '/tracker')
    def _canonical(path):
        return 0 if not any(x in path for x in _NOISE) else 1
    def _known(path):
        return any(k in path for k, _ in _LABEL_RULES)
    raw.sort(key=lambda x: (_canonical(x[0]), 0 if _known(x[0]) else 1, -x[2]['n']))
    # 去重: 同一 (total_ret, winrate, n, sharpe) 只留一条(规范块已排在前, 冲突时保留规范块)
    seen, results = set(), []
    for _, label, m in raw:
        key = (round(m['total_ret'], 3), round(m['winrate'], 2), m['n'], round(m['sharpe'], 3))
        if key in seen:
            continue
        seen.add(key)
        results.append((label, m))
    return results[:12]


# 标签优先级从高到低(具体路径优先于泛化路径); 用 '/forward' 而非 'forward',
# 避免误匹配文件名(如 volume_price_forward_validation_metrics 含 forward 字样)。
_LABEL_RULES = [
    ('in_sample_baseline', '低位绩优·样本内基线'),
    ('published_on_test', '低位绩优·样本外(TEST)'),
    ('published_on_train', '低位绩优·样本内(TRAIN)'),
    ('/forward', '低位绩优·真·前瞻(OOS)'),
    ('oos_published', '价量口诀·OOS(已发布组合)'),
    ('oos_pressure', '价量口诀·OOS(压力位卖点)'),
    ('oos_baseline', '价量口诀·OOS基线'),
    ('/in_sample', '价量口诀·样本内'),
    ('published_combo', '低位绩优·已发布组合'),
]


def _nice_label(path):
    for k, v in _LABEL_RULES:
        if k in path:
            return v
    # 兜底: 取末级 key
    parts = path.split('/')
    return parts[-1] if len(parts) > 1 else path


def gather_live():
    """调用 trade_journal 的 evaluate_data, 返回 type -> {win%, n, avg%} (取 10 日窗口)。"""
    try:
        from tools.trade_journal import evaluate_data
        sim_agg, _, sim_total, _ = evaluate_data(None)
    except Exception as e:
        print(f"[align] 读取实盘信号评估失败: {e}")
        return {}, 0
    out = {}
    for typ, d in sim_agg.items():
        w = d.get('10d')
        if w:
            out[typ] = dict(win=w['win'] * 100, n=w['n'], avg=w['avg'] * 100,
                            ft10=d.get('ft10'))
    return out, sim_total


def build_report():
    bt = gather_backtest()
    live, live_total = gather_live()
    L = ["📐 回测 vs 实盘 对齐报告 (统一口径)", "=" * 56]
    # 口径说明
    try:
        from tools.backtest_metrics import CONVENTIONS
        L.append("口径: total_ret=∏(1+ret)-1 | win=ret>0占比 | sharpe=年化近似 | "
                 "年化因子252")
    except Exception:
        pass
    L.append("")
    L.append("【Backtest 侧】")
    if not bt:
        L.append("  (opt_study 下无 *_metrics.json, 先跑 forward_validation 生成)")
    else:
        for label, m in bt:
            L.append(f"  · {label}: n={m['n']} 总收益{m['total_ret']:+.1f}% "
                     f"胜率{m['winrate']:.0f}% 夏普{m['sharpe']:.2f}")
    L.append("")
    L.append("【Live 侧(模拟信号 / 实盘)】")
    if not live:
        L.append(f"  (暂无模拟/实盘信号样本(共{live_total}条), 先跑 scan 积累)")
    else:
        for typ, d in sorted(live.items()):
            ft = d['ft10'] * 100 if d['ft10'] is not None else None
            L.append(f"  · [{typ}] 10日胜率{d['win']:.0f}% (n={d['n']}) "
                     f"均收益{d['avg']:+.1f}% 突破跟随={ft:.0f}%" if ft is not None
                     else f"  · [{typ}] 10日胜率{d['win']:.0f}% (n={d['n']}) 均收益{d['avg']:+.1f}%")
    L.append("")
    L.append("【对齐提示】")
    L.append("  - 回测为'历史点位'理想化收益(已含滑点/费); Live 为'信号后N日'真实收益。")
    L.append("  - 二者样本不同源: 回测=历史全窗口成交; Live=近期 scan 信号。直接比较仅看方向/量级。")
    L.append("  - 若 Live 10日胜率显著低于回测胜率, 说明近期市场/信号质量下滑, 应降仓(见仓位管理)。")
    return "\n".join(L), bt, live


def main():
    ap = argparse.ArgumentParser(description="回测 vs 实盘 对齐报告")
    ap.add_argument('--export', default=None, help="导出 HTML 路径")
    ap.add_argument('--push', action='store_true', help="额外 Bark 推送")
    a = ap.parse_args()
    text, bt, live = build_report()
    print(text)
    if a.export:
        html = ("<html><head><meta charset='utf-8'><style>"
                "body{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a}"
                "pre{font-size:13px;line-height:1.6;background:#f8fafc;border:1px solid #e3e8ee;"
                "border-radius:8px;padding:14px;white-space:pre-wrap}</style></head><body>"
                f"<h1>回测 vs 实盘 对齐报告</h1><pre>{text}</pre></body></html>")
        Path(a.export).write_text(html, encoding='utf-8')
        print(f"\n已导出 → {a.export}")
    if a.push:
        try:
            from tools.tracking_utils import bark_notify
            bark_notify("📐 回测对齐报告", text[:3800])
            print("[align] Bark 推送成功")
        except Exception as e:
            print("[align] Bark 推送失败:", e)


if __name__ == '__main__':
    main()
