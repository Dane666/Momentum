"""
日历择时(春季躁动/四月决断)有效性验证 —— 基于本仓库 kline_cache + fundamentals.

硬约束(务必在报告中说明):
  - 数据窗口仅 2024-06-03 ~ 2026-07-22 (~2年, 519日). 每个日历月仅 1~2 样本, 无法做统计显著性检验.
  - 无市值/股息率字段 -> 真·小盘 tilt 与 红利 tilt 不可测; 用可得因子近似(动量/低波/价值/质量).

本脚本产出:
  A. 等权全A日收益的"逐月季节性剖面"(各月平均日收益/月胜率) — 看日历效应方向
  B. 日期择时 overlay 对等权全A: spring(1-3月)满仓 / april(4月)空仓 / baseline(5-12月)满仓, vs 买入持有
  C. 因子倾斜可测部分: spring 动量 tilt vs 等权; april (ROE高+PB低+低波) tilt vs 等权
  D. 合成"日历策略"独立曲线(spring=动量篮子, april=质量价值低波篮子, baseline=等权) vs 等权
"""
import importlib.util, sys, sqlite3, json
from pathlib import Path
import pandas as pd, numpy as np

p = Path('.').resolve()
sys.path.insert(0, str(p/'opt_study'))
if 'momentum' not in sys.modules:
    spec = importlib.util.spec_from_file_location('momentum', p/'__init__.py', submodule_search_locations=[str(p)])
    m = importlib.util.module_from_spec(spec); sys.modules['momentum']=m; spec.loader.exec_module(m)

import harness as H
data_cache, sector_map, calendar = H.load_universe()
DB = "qlib_pro_v16.db"

# ---- 构建等权全A日收益面板 ----
# 用 close 构造 (date x code) panel, 日收益=close.pct_change, 等权=截面均值(跳过NaN)
closes = {}
for code, df in data_cache.items():
    s = df.set_index('trade_date')['close']
    closes[code] = s
panel = pd.DataFrame(closes)            # rows=dates, cols=codes
panel = panel.sort_index()
rets = panel.pct_change()                 # 519行(首行NaN), 后续按面板对齐
# 等权全A日收益
ew = rets.mean(axis=1, skipna=True).dropna()
ew.name = 'ew_market'
print(f"[panel] dates={panel.index[0].date()}..{panel.index[-1].date()} stocks={panel.shape[1]}")

# ---- 基本面(点态) ----
con = sqlite3.connect(DB)
fnd = pd.read_sql("SELECT code, avail_from, roe, bvps, eps_annualized FROM fundamentals", con)
con.close()
fnd['avail_from'] = pd.to_datetime(fnd['avail_from'])
# 每只股票按 avail_from 排序, 用于点态取"最新可得"财报
fnd = fnd.sort_values(['code','avail_from'])

def fundamentals_asof(date):
    """返回 date 当日每只股票最新可得财报(roe/bvps/eps_annualized)的 dict."""
    d = pd.Timestamp(date)
    sub = fnd[fnd['avail_from'] <= d]
    return sub.groupby('code').tail(1).set_index('code')

# ---- 因子: 动量 / 低波 ----
mom20 = panel.pct_change(20)                     # 20日动量
vol60 = rets.rolling(60).std()                   # 60日已实现波动(低波 proxy)

def month_of(idx):
    return pd.Timestamp(idx).month

def season_window(idx):
    mo = month_of(idx)
    if mo in (1,2,3): return 'spring'
    if mo == 4: return 'april'
    return 'baseline'

# =========================================================
# A. 逐月季节性剖面(等权全A日收益)
# =========================================================
monthly = {}
for mo in range(1,13):
    msk = ew.index.month == mo
    r = ew[msk]
    if len(r):
        monthly[mo] = dict(n_days=int(len(r)),
                           avg_daily_ret=round(float(r.mean())*100,4),
                           ann_ret=round(float(r.mean()*252)*100,2),
                           day_win=round(float((r>0).mean())*100,1),
                           total_ret=round(float((1+r).prod()-1)*100,2))
print("\n=== A. 逐月季节性剖面(等权全A) ===")
for mo in range(1,13):
    if mo in monthly:
        d=monthly[mo]; print(f"  {mo:2d}月: n={d['n_days']:3d} 日均={d['avg_daily_ret']:+.3f}% 日胜率={d['day_win']:4.1f}% 区间累计={d['total_ret']:+.1f}%")

# =========================================================
# 指标函数
# =========================================================
def metrics(ret_series):
    r = ret_series.dropna()
    if len(r)==0: return dict(n=0, ann=0, sharpe=0, maxdd=0, win=0)
    ann = r.mean()*252
    vol = r.std()*np.sqrt(252)
    sharpe = ann/vol if vol>0 else 0
    eq = (1+r).cumprod()
    maxdd = float((eq/eq.cummax()-1).min())*100
    win = float((r>0).mean())*100
    return dict(n=int(len(r)), ann=round(ann*100,2), sharpe=round(sharpe,3),
                maxdd=round(maxdd,2), win=round(win,1))

# =========================================================
# B. 日期择时 overlay(在等权全A上): april 空仓
# =========================================================
def timing_overlay(ret, flat_months=(4,)):
    out=[]
    for idx, v in ret.items():
        out.append(0.0 if month_of(idx) in flat_months else v)
    return pd.Series(out, index=ret.index).dropna()

ew_bh = metrics(ew)
ew_timing = metrics(timing_overlay(ew, (4,)))             # 仅四月空仓
ew_timing_springonly = metrics(timing_overlay(ew, (4,5,6,7,8,9,10,11,12)))  # 仅春季持仓
print("\n=== B. 日期择时 overlay(等权全A) ===")
print(f"  买入持有(B&H):        {ew_bh}")
print(f"  四月空仓(其余满仓):   {ew_timing}")
print(f"  仅春季持仓(其余空仓): {ew_timing_springonly}")

# =========================================================
# C. 因子倾斜(可测部分)
# =========================================================
def tilt_basket(ret_panel, score_panel, top_frac=0.2, min_n=30):
    """按 score_panel 每日横截排名, 持多前 top_frac, 等权, 日收益=这些票截面均值."""
    days = ret_panel.index
    out=[]
    for d in days:
        sc = score_panel.loc[d].dropna()
        if len(sc) < min_n: 
            out.append(np.nan); continue
        k = max(int(len(sc)*top_frac), 1)
        top = sc.sort_values(ascending=False).head(k).index
        out.append(ret_panel.loc[d, top].mean(skipna=True))
    return pd.Series(out, index=days).dropna()

# spring 动量 tilt (mask 用 panel 索引 519, 与 rets/mom20/score 对齐; ew 窗口单独用 ew.index)
spring_mask = panel.index.month.isin([1,2,3])
april_mask = panel.index.month == 4

mom_spring = tilt_basket(rets[spring_mask], mom20[spring_mask])
ew_spring  = ew[ew.index.month.isin([1,2,3])]
print("\n=== C. 因子倾斜(窗口内 vs 等权) ===")
print(f"  SPRING(1-3月) 动量tilt: {metrics(mom_spring)}  | 等权: {metrics(ew_spring)}")

# april 质量+价值+低波 tilt: score = ROE(z) - PB(z) - vol(z)  (高roe/低pb/低波 更好)
# 需要 PB = close/bvps, 用点态基本面
pb_panel = pd.DataFrame(index=panel.index, columns=panel.columns, dtype=float)
for d in panel.index:
    f = fundamentals_asof(d)
    for code in panel.columns:
        close = panel.loc[d, code]
        bvps = f['bvps'].get(code, np.nan) if code in f.index else np.nan
        pb_panel.loc[d, code] = close/bvps if (pd.notna(bvps) and bvps>0) else np.nan
pb_panel = pb_panel.astype(float)
# z-score 横截标准化(每日)
def zscore_panel(x):
    return (x - x.mean())/x.std()
roe_panel = pd.DataFrame(index=panel.index, columns=panel.columns, dtype=float)
for d in panel.index:
    f = fundamentals_asof(d)
    for code in panel.columns:
        roe_panel.loc[d, code] = f['roe'].get(code, np.nan) if code in f.index else np.nan
roe_panel = roe_panel.astype(float)

score_april = (zscore_panel(roe_panel) - zscore_panel(pb_panel) - zscore_panel(vol60))
april_score_win = score_april[april_mask]
mom_april = tilt_basket(rets[april_mask], april_score_win)
ew_april = ew[ew.index.month == 4]
print(f"  APRIL(4月)  质量价值低波tilt: {metrics(mom_april)}  | 等权: {metrics(ew_april)}")

# =========================================================
# D. 合成日历策略(独立曲线)
#   spring=动量篮子 / april=质量价值低波篮子 / baseline=等权
# =========================================================
def calendar_strategy_daily():
    out=[]
    idxs = ew.index
    for d in idxs:
        mo = month_of(d)
        if mo in (1,2,3):
            sc = mom20.loc[d].dropna()
            if len(sc)>=30:
                k=max(int(len(sc)*0.2),1); top=sc.sort_values(ascending=False).head(k).index
                out.append(rets.loc[d, top].mean(skipna=True)); continue
        if mo==4:
            sc = score_april.loc[d].dropna()
            if len(sc)>=30:
                k=max(int(len(sc)*0.2),1); top=sc.sort_values(ascending=False).head(k).index
                out.append(rets.loc[d, top].mean(skipna=True)); continue
        out.append(ew.loc[d])   # baseline 等权
    return pd.Series(out, index=idxs).dropna()

cal = calendar_strategy_daily()
print("\n=== D. 合成日历策略(独立曲线) ===")
print(f"  日历策略: {metrics(cal)}")
print(f"  等权全A:  {metrics(ew)}")

# ---- 汇总 JSON ----
result = dict(
    data_window=dict(start=str(panel.index[0].date()), end=str(panel.index[-1].date()),
                     n_days=int(len(panel)), n_stocks=int(panel.shape[1])),
    caveat="仅~2年历史, 每日历月1-2样本, 无统计显著性; 无市值/股息率, 小盘/红利tilt不可测",
    monthly_profile=monthly,
    timing_overlay=dict(buyhold=ew_bh, april_flat=ew_timing, spring_only=ew_timing_springonly),
    factor_tilt=dict(spring_momentum=metrics(mom_spring), spring_ew=metrics(ew_spring),
                     april_quality_value_lowvol=metrics(mom_april), april_ew=metrics(ew_april)),
    calendar_strategy=dict(calendar=metrics(cal), ew=metrics(ew)),
)
with open('opt_study/seasonal_validation.json','w',encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("\n[done] 写出 opt_study/seasonal_validation.json")
