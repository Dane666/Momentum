"""
长期日历效应检验 —— 直接读 kline_cache 全样本(1995-2026, 所有A股等权),
绕过 harness.load_universe 的近期流动性过滤, 做有统计意义的检验.

产出:
  A. 逐月季节性剖面(跨全部年份, 每月~30*21样本) — 看日历效应方向
  B. 日期择时 overlay(等权全样本): 四月空仓 / 仅春季持仓, vs 买入持有
  C. Welch t 检验: 春季(1-3月)日均收益 vs 其余; 四月 vs 其余 — 给 p 值判断显著性
注: 等权含全部曾上市A股(缓解幸存者偏差, 但含僵尸/仙股, 为朴素市场代理).
"""
import sqlite3, json
import pandas as pd, numpy as np

DB = "qlib_pro_v16.db"
con = sqlite3.connect(DB)
print("读取 kline_cache (code, trade_date, close)...", flush=True)
df = pd.read_sql("SELECT code, trade_date, close FROM kline_cache ORDER BY code, trade_date", con)
con.close()
df['trade_date'] = pd.to_datetime(df['trade_date'])
df['ret'] = df.groupby('code')['close'].pct_change()
df = df.dropna(subset=['ret'])
print(f"  有效日收益样本: {len(df):,} 行", flush=True)

# 等权全A日收益(按交易日截面均值)
ew = df.groupby('trade_date')['ret'].mean().sort_index()
ew.index = pd.to_datetime(ew.index)
print(f"  交易日: {ew.index[0].date()} ~ {ew.index[-1].date()}  n={len(ew)}", flush=True)

def metrics(r):
    r = pd.Series(r).dropna()
    if len(r)==0: return dict(n=0, ann=0.0, sharpe=0.0, maxdd=0.0, win=0.0)
    ann = r.mean()*252; vol = r.std()*np.sqrt(252)
    sharpe = ann/vol if vol>0 else 0.0
    eq = (1+r).cumprod(); maxdd = float((eq/eq.cummax()-1).min())*100
    return dict(n=int(len(r)), ann=round(ann*100,2), sharpe=round(sharpe,3),
                maxdd=round(maxdd,2), win=round(float((r>0).mean())*100,1))

def welch_t(a, b):
    a=np.asarray(a,float); b=np.asarray(b,float)
    if len(a)<2 or len(b)<2: return None, None
    ma,mb=a.mean(),b.mean(); va,vb=a.var(ddof=1),b.var(ddof=1)
    se=np.sqrt(va/len(a)+vb/len(b)); t=(ma-mb)/se if se>0 else 0.0
    # 近似自由度(Welch-Satterthwaite)
    dfree=(va/len(a)+vb/len(b))**2/((va/len(a))**2/(len(a)-1)+(vb/len(b))**2/(len(b)-1))
    return round(float(t),3), round(float(dfree),1)

# ============ A. 逐月剖面(跨全部年份) ============
print("\n=== A. 逐月季节性剖面(全样本, 跨年份) ===")
monthly={}
for mo in range(1,13):
    r = ew[ew.index.month==mo]
    monthly[mo]=dict(n=int(len(r)), avg_daily=round(float(r.mean())*100,4),
                     ann=round(float(r.mean()*252)*100,2),
                     day_win=round(float((r>0).mean())*100,1))
    print(f"  {mo:2d}月: n={monthly[mo]['n']:5d} 日均={monthly[mo]['avg_daily']:+.4f}% 日胜率={monthly[mo]['day_win']:4.1f}% 年化={monthly[mo]['ann']:+.1f}%")

# ============ B. 日期择时 overlay ============
spring = ew[ew.index.month.isin([1,2,3])]
april = ew[ew.index.month==4]
def flat_months(ret, months):
    return pd.Series([0.0 if pd.Timestamp(i).month in months else v for i,v in ret.items()], index=ret.index).dropna()
print("\n=== B. 日期择时 overlay(全样本等权) ===")
print(f"  买入持有:        {metrics(ew)}")
print(f"  四月空仓:        {metrics(flat_months(ew,(4,)))}")
print(f"  仅春季持仓:      {metrics(flat_months(ew,(4,5,6,7,8,9,10,11,12)))}")

# ============ C. Welch t 检验 ============
print("\n=== C. Welch t 检验(日均收益差) ===")
t_sp, df_sp = welch_t(spring.values, ew.drop(spring.index).values)
t_ap, df_ap = welch_t(april.values, ew.drop(april.index).values)
print(f"  春季(1-3月) vs 其余: 春季日均={spring.mean()*100:+.4f}% 其余={ew.drop(spring.index).mean()*100:+.4f}%  t={t_sp} df={df_sp}")
print(f"  四月 vs 其余:        四月日均={april.mean()*100:+.4f}% 其余={ew.drop(april.index).mean()*100:+.4f}%  t={t_ap} df={df_ap}")
print("  (|t|>~2 且样本大时近似 p<0.05; 但日收益非正态, 结论需谨慎)")

result=dict(
    data="kline_cache 全样本(1995-2026, 所有曾上市A股等权)",
    n_days=int(len(ew)), window=[str(ew.index[0].date()), str(ew.index[-1].date())],
    monthly_profile=monthly,
    timing_overlay=dict(buyhold=metrics(ew), april_flat=metrics(flat_months(ew,(4,))),
                        spring_only=metrics(flat_months(ew,(4,5,6,7,8,9,10,11,12)))),
    welch=dict(spring_vs_rest=dict(t=t_sp, df=df_sp, spring_mean_daily=round(float(spring.mean())*100,4),
                                   rest_mean_daily=round(float(ew.drop(spring.index).mean())*100,4)),
               april_vs_rest=dict(t=t_ap, df=df_ap, april_mean_daily=round(float(april.mean())*100,4),
                                  rest_mean_daily=round(float(ew.drop(april.index).mean())*100,4))),
)
with open('opt_study/seasonal_longrun.json','w',encoding='utf-8') as f:
    json.dump(result,f,ensure_ascii=False,indent=2)
print("\n[done] 写出 opt_study/seasonal_longrun.json")
