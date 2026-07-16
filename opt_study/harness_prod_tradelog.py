# -*- coding: utf-8 -*-
"""
生产动量策略 逐笔交易记录生成
=================================
目的: 生产动量策略(harness.py 基准打分 + 套牢盘过滤 + 自适应退出)此前只输出组合净值,
      没有逐笔明细。这里为它生成逐笔交易记录(10万本金, 等权分配), 用于与 C 方案
      (harness_c_enhanced.py gap_h2_lu0_trapFalse) 做参数/胜率/收益/逐笔对比。

口径(与 harness.py 基准 hold3_shift0 完全一致, 该口径下总收益 +33.98%):
  - 底池: 每个调仓日成交额 Top POOL_SIZE(150) 只
  - 打分: score_baseline (原版 AlphaModel)
  - 过滤: select_picks —— sharpe>1.0, trapped<=0.10, RSI<80, 每行业<=2, 总数<=3
  - 退出: ExitRuleEngine(adaptive=True) 自适应止盈止损 (fwd_ret 已含滑点0.8%)
  - 账户: 每个调仓日把"当前总权益"等权分给当日选出的 <=3 只票; 每只持有到 fwd_ret 实现;
          下个调仓日再重新等权分配(组合再平衡, 与原框架的均值净值口径一致)
  - 窗口: calendar[-(250+3):-3][::3]  (与 hold5/hold3 基准同源)
"""
from __future__ import annotations
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import harness as H
from momentum import config as cfg
from momentum.risk import ExitRuleEngine

INIT_CAPITAL = 100_000.0


def main():
    print("[1/3] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]} 股票={len(data_cache)}", flush=True)

    exit_engine = ExitRuleEngine(adaptive=getattr(cfg, "USE_ADAPTIVE_EXIT", True))
    min_amount = cfg.MIN_AMOUNT

    day_cache = {}

    def get_daily_top(t_date, top_n):
        tt = pd.Timestamp(t_date).normalize()
        amts = []
        for code, g in data_cache.items():
            dd = g[g["trade_date"] == tt]
            if not dd.empty:
                a = dd["amount"].iloc[0]
                if pd.notna(a) and a > 0:
                    amts.append((code, a))
        amts.sort(key=lambda x: x[1], reverse=True)
        return [c for c, _ in amts[:top_n]]

    def day_getter(t_date, hold_period):
        key = (str(t_date)[:10], hold_period)
        if key in day_cache:
            return day_cache[key]
        top = get_daily_top(t_date, cfg.POOL_SIZE)
        results = []
        for code in top:
            g = data_cache.get(code)
            if g is None:
                continue
            rec = H.simulate_day(code, code, sector_map.get(code, "其它"), g,
                                 t_date, hold_period, exit_engine, min_amount)
            if rec:
                results.append(rec)
        day_cache[key] = results
        return results

    all_metrics = {}
    for HOLD in (5, 3):
        # 窗口: 与 harness.py holdN_shift0 完全一致
        test_dates = calendar[-(cfg.BACKTEST_DAYS_DEFAULT + HOLD):-HOLD]
        reb_dates = test_dates[::HOLD]
        print(f"[2/3] 逐笔模拟(基准打分, 持{HOLD}天, {len(reb_dates)}个调仓日) ...", flush=True)

        equity = INIT_CAPITAL
        ops = []
        seq = 0
        trades = []          # 每笔 fwd_ret
        equity_curve = [INIT_CAPITAL]
        for t_date in reb_dates:
            dstr = str(t_date)[:10]
            day_results = day_getter(t_date, HOLD)
            if not day_results:
                equity_curve.append(equity)
                continue
            df_scored = H.score_baseline(day_results)
            picks, eligible = H.select_picks(df_scored, {})   # 基准变体(默认阈值)
            if not picks:
                seq += 1
                ops.append({"序号": seq, "调仓日": dstr, "代码": "", "名称行业": "",
                            "买入价": "", "投入资金": "", "前向收益%": "", "持有天数": "",
                            "退出原因": "", "退出日": "", "盈亏元": "", "账户净值": round(equity, 2),
                            "备注": f"无合格标的(候选{eligible})"})
                equity_curve.append(equity)
                continue
            alloc = equity / len(picks)      # 等权分配当前权益
            period_pnl = 0.0
            period_rets = []
            for _, row in pd.DataFrame(picks).iterrows():
                code = row["code"]; sector = row["sector"]
                entry_price = float(row["close"])
                g = data_cache[code]
                t_idx = g.index[g["trade_date"] == pd.Timestamp(t_date).normalize()]
                if len(t_idx) == 0:
                    continue
                ei = int(t_idx[0])
                fwd_ret, reason, hold_days, exit_date = exit_engine.simulate_exit(
                    entry_price=entry_price, df=g, entry_idx=ei,
                    hold_period=HOLD, slippage=cfg.SLIPPAGE)
                pnl = alloc * fwd_ret
                period_pnl += pnl
                period_rets.append(fwd_ret)
                trades.append(fwd_ret)
                seq += 1
                ops.append({
                    "序号": seq, "调仓日": dstr, "代码": str(code),
                    "名称行业": sector,
                    "买入价": round(entry_price, 3),
                    "投入资金": round(alloc, 2),
                    "前向收益%": round(fwd_ret * 100, 2),
                    "持有天数": hold_days,
                    "退出原因": reason,
                    "退出日": str(exit_date)[:10],
                    "盈亏元": round(pnl, 2),
                    "账户净值": "",
                    "备注": f"等权1/{len(picks)}仓",
                })
            equity += period_pnl
            for op in ops[-len(period_rets):]:
                op["账户净值"] = round(equity, 2)
            equity_curve.append(equity)

        # ---- 指标 ----
        arr = np.array(equity_curve, dtype=float)
        total_ret = (arr[-1] / INIT_CAPITAL - 1) * 100
        peak = np.maximum.accumulate(arr)
        mdd = np.max((peak - arr) / (peak + 1e-9)) * 100
        rets = np.diff(arr) / arr[:-1]
        ppy = 252 / HOLD
        sharpe = (rets.mean() / (rets.std() + 1e-9)) * np.sqrt(ppy) if rets.std() > 0 else 0.0
        win_rate = sum(1 for r in trades if r > 0) / len(trades) * 100 if trades else 0.0
        annual = (arr[-1] / INIT_CAPITAL) ** (252 / cfg.BACKTEST_DAYS_DEFAULT) * 100 - 100
        avg_win = np.mean([r for r in trades if r > 0]) * 100 if any(r > 0 for r in trades) else 0.0
        avg_loss = np.mean([r for r in trades if r <= 0]) * 100 if any(r <= 0 for r in trades) else 0.0
        avg_hold = np.mean([o["持有天数"] for o in ops if isinstance(o.get("持有天数"), (int, float))]) if ops else 0.0

        metrics = {
            "期末净值": round(float(arr[-1]), 2),
            "总收益%": round(float(total_ret), 2),
            "年化%": round(float(annual), 2),
            "夏普": round(float(sharpe), 3),
            "最大回撤%": round(float(mdd), 2),
            "胜率%": round(float(win_rate), 1),
            "交易笔数": len(trades),
            "调仓日数": len(reb_dates),
            "平均盈利%": round(float(avg_win), 2),
            "平均亏损%": round(float(avg_loss), 2),
            "平均持有天数": round(float(avg_hold), 2),
        }
        all_metrics[f"hold{HOLD}"] = metrics
        print(f"[3/3] hold{HOLD} 指标:", json.dumps(metrics, ensure_ascii=False), flush=True)

        df = pd.DataFrame(ops)
        csv_path = HERE / f"prod_momentum_tradelog_hold{HOLD}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        reasons = df[df["退出原因"] != ""]["退出原因"].value_counts().to_dict()
        all_metrics[f"hold{HOLD}"]["退出原因分布"] = reasons
        print(f"完成 -> {csv_path.name} ({len(df)}行)")

    (HERE / "prod_momentum_tradelog_metrics.json").write_text(
        json.dumps(all_metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> prod_momentum_tradelog_metrics.json")


if __name__ == "__main__":
    main()
