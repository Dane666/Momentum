# -*- coding: utf-8 -*-
"""精确分桶: 首日热门(consec==1) vs 持续2天(consec==2) vs 持续>=3天
   仅对最优 top_k=8, hold∈{3,5} 跑精确桶, 直接回答"首日 vs 持续多天 谁胜率高"。"""
import sys, json
from pathlib import Path
from collections import defaultdict
import numpy as np, pandas as pd

HERE = Path(__file__).resolve().parent
TESTS = HERE.parent
sys.path.insert(0, str(HERE)); sys.path.insert(0, str(TESTS))
import harness as H
from momentum import config as cfg
from momentum.risk import ExitRuleEngine
import harness_sector as HS

def picks_exact(day_results, hot_set, consec_map, exact):
    if exact == "ge3":
        cands = [r for r in day_results if r["sector"] in hot_set and consec_map.get(r["sector"],0) >= 3]
    else:
        c = int(exact)
        cands = [r for r in day_results if r["sector"] in hot_set and consec_map.get(r["sector"],0) == c]
    if not cands:
        return []
    df = H.score_baseline(cands)
    picks, _ = H.select_picks(df, {"max_picks": 1, "max_sector": 1})
    return picks

def run(top_k, holds, buckets):
    data_cache, sector_map, calendar = H.load_universe()
    exit_engine = ExitRuleEngine(adaptive=getattr(cfg, "USE_ADAPTIVE_EXIT", True))
    min_amount = cfg.MIN_AMOUNT
    day_cache = {}
    def get_daily_top(t_date, top_n):
        tt = pd.Timestamp(t_date).normalize(); amts=[]
        for code,g in data_cache.items():
            dd=g[g["trade_date"]==tt]
            if not dd.empty:
                a=dd["amount"].iloc[0]
                if pd.notna(a) and a>0: amts.append((code,a))
        amts.sort(key=lambda x:x[1],reverse=True)
        return [c for c,_ in amts[:top_n]]
    def dcg(t_date, hold_period):
        key=(str(t_date)[:10],hold_period)
        if key in day_cache: return day_cache[key]
        res=[]
        for code in get_daily_top(t_date, cfg.POOL_SIZE):
            g=data_cache.get(code)
            if g is None: continue
            rec=H.simulate_day(code,code,sector_map.get(code,"其它"),g,t_date,hold_period,exit_engine,min_amount)
            if rec: res.append(rec)
        day_cache[key]=res; return res
    hot_by_date, consec_by_date, _ = HS.build_sector_heat(data_cache, sector_map, calendar, top_k)
    out=[]
    for hold in holds:
        for b in buckets:
            cfgs={}
            for shift in HS.WINDOW_SHIFTS:
                td=HS.slice_test_dates(calendar,hold,shift)
                if not td: continue
                reb=td[::hold]; eq=[1.0]; daily=[]; tc=0; wc=0; do=[]
                for t in reb:
                    recs=dcg(t,hold)
                    if not recs: eq.append(eq[-1]); daily.append(0.0); do.append(str(t)[:10]); continue
                    picks=picks_exact(recs,hot_by_date[t],consec_by_date[t],b)
                    if picks:
                        pr=float(np.mean([p["fwd_ret"] for p in picks]))
                        tc+=len(picks); wc+=int((np.array([p["fwd_ret"] for p in picks])>0).sum())
                        eq.append(eq[-1]*(1+pr)); daily.append(pr)
                    else: eq.append(eq[-1]); daily.append(0.0)
                    do.append(str(t)[:10])
                m=H.compute_metrics(eq,daily,tc,wc,hold,do)
                if m: cfgs[f"hold{hold}_shift{shift}"]={k:m[k] for k in ("profit_pct","sharpe","win_rate","max_dd","trade_count","final_nav")}
            if cfgs:
                rows=list(cfgs.values())
                out.append({"bucket":b,"hold":hold,
                    "avg_profit":round(float(np.mean([r["profit_pct"] for r in rows])),2),
                    "avg_sharpe":round(float(np.mean([r["sharpe"] for r in rows])),3),
                    "avg_win_rate":round(float(np.mean([r["win_rate"] for r in rows])),2),
                    "avg_max_dd":round(float(np.mean([r["max_dd"] for r in rows])),2),
                    "avg_trades":round(float(np.mean([r["trade_count"] for r in rows])),1),
                    "main":cfgs.get("hold5_shift0",{})})
    return out

if __name__=="__main__":
    res=run(top_k=8, holds=[5,3], buckets=["1","2","ge3"])
    print(json.dumps(res,ensure_ascii=False,indent=2))
    Path(HERE/"sector_exact.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8")
