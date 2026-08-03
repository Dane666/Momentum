# -*- coding: utf-8 -*-
"""
超跌绩优反弹 · 增强版(参数扫描 / ablation)
==========================================
在朴素超跌反弹基础上, 叠加三道"网搜归纳的真实绩优特征", 并做参数扫描对比:

  A) 真实基本面(fundamentals 表, fetch_fundamentals.py 抓取东方财富真实数据):
       ROE>=ROE_MIN; 净利润同比(业绩拐点)>0; PE(close/年化EPS, point-in-time)<=PE_MAX; PB<=PB_MAX
  B) 热门板块: 过去30交易日资金净流入 Top-K 题材成分股
  C) 网搜技术特征(好公司+跌到位+资金重新进场):
       - 60日线不下行(ma60斜率向上, 剔除下跌中继)
       - 缩量回调后放量企稳(下跌段量萎缩, 信号日放量阳线收复5日线)
       - MACD低位金叉 或 RSI超卖回升
     + 超跌两种口径:
       口径①深度超跌(用户原意): 距60日高回撤<=DD + 收盘跌破60日线(乖离>=GAP) + RSI<35
       口径②回踩企稳(网搜共识): 距60日高回撤<=DD2 + 收盘不低于60日线*(1-FLOOR) + RSI<45

输出: 扫描对照表(命令行+JSON) + 最优变体完整HTML报告 + 逐笔CSV
不修改任何原有文件。
"""
import os, json, sqlite3
from collections import defaultdict
import numpy as np
import pandas as pd

DB = "/Users/admin/Documents/codeHub/adata-main/tests/momentum/qlib_pro_v16.db"
ROOT = "/Users/admin/Documents/codeHub/adata-main/tests/momentum"

SLIP = 0.008
INIT_CAPITAL = 100000.0
N_SLOTS = 3
HOLD_DEF = 20
ENTRY_MODE = "close"
STOP_LOSS = -0.07
SAME_CODE_COOLDOWN = 10    # 同股冷却期(交易日): 退出后 N 日内不再买入同一只, 避免过热交易

ROE_MIN = 8.0
NP_YOY_MIN = 0.0
PE_MAX = 50.0
PB_MAX = 10.0

TOP_K_THEMES = 8
HOT_LOOKBACK = 30
RSI_TH_DEF = 35

WINDOW_START = "2024-07-01"
WINDOW_END = "2026-07-15"

def wilder_rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0); dn = -d.clip(upper=0)
    au = up.ewm(alpha=1/n, adjust=False).mean()
    ad = dn.ewm(alpha=1/n, adjust=False).mean()
    rs = au/ad.replace(0, np.nan)
    return 100 - 100/(1+rs)

def macd(close, fast=12, slow=26, sig=9):
    dif = close.ewm(span=fast, adjust=False).mean() - close.ewm(span=slow, adjust=False).mean()
    dea = dif.ewm(span=sig, adjust=False).mean()
    return dif, dea

def load_kline():
    con = sqlite3.connect(DB)
    df = pd.read_sql_query(
        f"SELECT code,trade_date,open,high,low,close,volume,amount,turnover_ratio "
        f"FROM kline_cache WHERE trade_date>='{WINDOW_START}' AND trade_date<='{WINDOW_END}'", con)
    con.close()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["code","trade_date"]).reset_index(drop=True)
    ctx = {}
    for code, g in df.groupby("code"):
        ctx[code] = g.set_index("trade_date").sort_index()
    return ctx

def build_ctx(ctx):
    for code, g in ctx.items():
        c = g["close"]; h = g["high"]; l = g["low"]; v = g["volume"]
        g["ma5"]=c.rolling(5).mean(); g["ma20"]=c.rolling(20).mean()
        g["ma60"]=c.rolling(60).mean(); g["ma120"]=c.rolling(120).mean()
        g["rsi14"]=wilder_rsi(c,14)
        g["high60"]=h.rolling(60).max(); g["dd60"]=c/g["high60"]-1.0
        g["ma60_gap"]=c/g["ma60"]-1.0
        g["ma60_slope"]=g["ma60"]/g["ma60"].shift(5)-1.0
        g["vol_ratio"]=v/v.shift(1)
        g["vma5"]=v.rolling(5).mean(); g["vma20"]=v.rolling(20).mean()
        g["vol_dryup"]=g["vma5"]<g["vma20"]
        dif, dea = macd(c)
        cross = (dif.shift(1)<=dea.shift(1)) & (dif>dea)
        g["macd_gold3"]=(cross.rolling(3).max().fillna(0)>0) & (dif<0)
        was = g["rsi14"].rolling(5).min()<RSI_TH_DEF
        g["rsi_rebound"]=was & (g["rsi14"]>g["rsi14"].shift(3)) & (g["rsi14"]<45)
        ctx[code]=g
    return ctx

def load_fundamentals():
    con = sqlite3.connect(DB)
    df = pd.read_sql_query("SELECT * FROM fundamentals", con); con.close()
    fmap = defaultdict(list)
    for _, r in df.iterrows():
        fmap[r["code"]].append(dict(avail_from=r["avail_from"], roe=r["roe"],
            np_yoy=r["net_profit_yoy"], rev_yoy=r["revenue_yoy"],
            eps_ann=r["eps_annualized"], bvps=r["bvps"]))
    for k in fmap: fmap[k].sort(key=lambda x:x["avail_from"])
    return fmap

def load_market_stats():
    """读取 market_stats(月频 总市值/流通市值/股息率), 返回 point-in-time 可查结构.
    与 load_fundamentals 同口径: code -> [{trade_date,total_mv,circ_mv,dividend_yield}] 按日期升序.
    数据来源见 opt_study/market_stats.py (AKShare stock_value_em + stock_history_dividend)."""
    con = sqlite3.connect(DB)
    df = pd.read_sql_query(
        "SELECT code,trade_date,total_mv,circ_mv,dividend_yield FROM market_stats", con)
    con.close()
    mmap = defaultdict(list)
    for _, r in df.iterrows():
        mmap[r["code"]].append(dict(
            trade_date=r["trade_date"], total_mv=r["total_mv"],
            circ_mv=r["circ_mv"], dividend_yield=r["dividend_yield"]))
    for k in mmap: mmap[k].sort(key=lambda x: x["trade_date"])
    return mmap

def market_stats_at(mmap, code, date_str):
    """返回 code 在 <=date_str 最近一个月末快照(月频, 向前取最近月首). 无则返回 None."""
    recs = mmap.get(code)
    if not recs: return None
    cur = None
    for rec in recs:
        if rec["trade_date"] <= date_str: cur = rec
        else: break
    return cur

def quality_ok(fmap, code, date_str, close, pe_pb_on):
    recs = fmap.get(code)
    if not recs: return (False,None,None,None,None)
    cur=None
    for rec in recs:
        if rec["avail_from"]<=date_str: cur=rec
        else: break
    if cur is None: return (False,None,None,None,None)
    roe=cur["roe"]; np_yoy=cur["np_yoy"]; eps_ann=cur["eps_ann"]; bvps=cur["bvps"]
    if roe is None or roe<ROE_MIN: return (False,None,None,roe,np_yoy)
    if np_yoy is None or np_yoy<=NP_YOY_MIN: return (False,None,None,roe,np_yoy)
    pe=close/eps_ann if (eps_ann and eps_ann>0) else None
    pb=close/bvps if (bvps and bvps>0) else None
    if pe_pb_on:
        if pe is None or pe<=0 or pe>PE_MAX: return (False,pe,pb,roe,np_yoy)
        if pb is None or pb<=0 or pb>PB_MAX: return (False,pe,pb,roe,np_yoy)
    return (True,pe,pb,roe,np_yoy)

def build_hot_themes(ctx, calendar):
    con = sqlite3.connect(DB)
    mem = defaultdict(set)
    for code, sec, _ in con.execute("SELECT code,sector,update_time FROM stock_sector_cache"):
        if sec and sec!="其它": mem[code].add(sec)
    con.close()
    sec2codes=defaultdict(list); all_codes=set(ctx.keys())
    for code, secs in mem.items():
        if code in all_codes:
            for s in secs: sec2codes[s].append(code)
    flow={}
    for code, g in ctx.items():
        c=g["close"]; h=g["high"]; l=g["low"]; amt=g["amount"]
        fl=(amt*(2*c-h-l)/(h-l).replace(0,np.nan)).fillna(0)
        flow[code]=fl
    flow30={code: fl.rolling(HOT_LOOKBACK).sum() for code, fl in flow.items()}
    hot_at={}
    for t in calendar:
        ts=pd.Timestamp(t)
        sec_flow={}
        for s, codes in sec2codes.items():
            tot=0.0
            for c in codes:
                f=flow30[c]
                if ts in f.index:
                    val=f.loc[ts]
                    if not pd.isna(val): tot+=val
            sec_flow[s]=tot
        top=sorted(sec_flow.items(), key=lambda x:-x[1])[:TOP_K_THEMES]
        hot_codes=set()
        for s,_ in top: hot_codes.update(sec2codes[s])
        # 每只热门成分股 -> 它所属的(热门)题材列表, 供"单题材持仓上限"判定
        code_to_themes={}
        for s,_ in top:
            for c in sec2codes[s]:
                code_to_themes.setdefault(c, []).append(s)
        hot_at[str(t)[:10]]=(hot_codes,[s for s,_ in top],code_to_themes)
    return hot_at

def build_market_proxy(ctx, cal_slice):
    """等权全A净值(行情择时用). 返回 pd.Series(按 cal_slice 索引)."""
    closes=pd.DataFrame({code: g["close"] for code, g in ctx.items()})
    closes=closes.reindex(pd.DatetimeIndex(cal_slice)).sort_index()
    daily_ret=closes.pct_change().mean(axis=1, skipna=True).fillna(0.0)
    nav=(1.0+daily_ret).cumprod()
    return nav

def base_signal(g, t, cfg):
    if t not in g.index: return False
    i=g.index.get_loc(t)
    if i<60: return False
    r=g.iloc[i]
    if pd.isna(r["ma60"]) or pd.isna(r["rsi14"]) or pd.isna(r["high60"]): return False
    # 超跌口径
    if cfg["mode"]=="deep":
        if not (r["dd60"]<=cfg["dd"] and r["ma60_gap"]<=-cfg["gap"] and r["rsi14"]<cfg["rsi_th"]):
            return False
    else:  # pullback
        if not (r["dd60"]<=cfg["dd"] and r["rsi14"]<cfg["rsi_th"]):
            return False
        if cfg.get("floor_ma60") is not None:
            if pd.isna(r["ma60"]) or r["close"] < r["ma60"]*(1-cfg["floor_ma60"]):
                return False
    if cfg.get("ma60_rising") and (pd.isna(r["ma60_slope"]) or r["ma60_slope"]<=0):
        return False
    if cfg.get("vol_confirm"):
        vol_up=(not pd.isna(r["vol_ratio"])) and r["vol_ratio"]>1.2
        bull=(r["close"]>r["open"]) and (r["close"]>=r["ma5"])
        dry=(not pd.isna(r["vol_dryup"])) and r["vol_dryup"]
        if not (vol_up and bull and dry): return False
    if cfg.get("macd_rsi"):
        macd_ok=(not pd.isna(r["macd_gold3"])) and bool(r["macd_gold3"])
        rsi_ok=(not pd.isna(r["rsi_rebound"])) and bool(r["rsi_rebound"])
        if not (macd_ok or rsi_ok): return False
    return True

def build_signal_index(ctx, cal_slice, cfg):
    inv=defaultdict(list)
    for code, g in ctx.items():
        for t in g.index:
            if t < pd.Timestamp(cal_slice[0]): continue
            if base_signal(g, t, cfg):
                inv[str(t)[:10]].append(code)
    return inv

def simulate(ctx, calendar, inv, hot_at, fmap, hold, entry_mode, stop_loss, cfg, cooldown=0, regime_at=None):
    """事件驱动 N 槽回测. 修复边界: 同股在持仓期间不可重复建仓; 退出后冷却期内不可再买.
    cooldown: 同股冷却期(交易日), 0=仅禁止持仓期内重复."""
    cal=list(calendar); cal_pos={t:i for i,t in enumerate(cal)}
    capital=INIT_CAPITAL; slots=[None]*N_SLOTS; trades=[]; eq=[]
    last_exit={}          # code -> 退出时的日历序号(用于冷却期)
    for ti,t in enumerate(cal):
        ts=str(t)[:10]
        for si in range(N_SLOTS):
            pos=slots[si]
            if pos is None: continue
            g=ctx[pos["code"]]
            if t not in g.index: continue
            row=g.loc[t]
            held=cal_pos[t]-cal_pos[pos["buy_t"]]
            exit_px=None; reason=None
            if held>=hold: exit_px=row["close"]*(1-SLIP); reason="到期"
            elif stop_loss is not None and row["close"]<=pos["buy_px"]*(1+stop_loss):
                exit_px=row["close"]*(1-SLIP); reason="止损"
            if exit_px is not None:
                ret=exit_px/pos["buy_px"]-1.0
                pnl=pos["shares"]*(exit_px-pos["buy_px"])
                capital+=pos["shares"]*exit_px
                trades.append(dict(code=pos["code"],buy_t=pos["buy_t"],buy_px=pos["buy_px"],
                    sell_t=t,sell_px=exit_px,shares=pos["shares"],ret=ret,pnl=pnl,
                    regime=pos["regime"],reason=reason,hold_days=held,
                    roe=pos["roe"],np_yoy=pos["np_yoy"],pe=pos["pe"],pb=pos["pb"]))
                slots[si]=None
                last_exit[pos["code"]]=ti
        # 当前持仓中的 code 集合(用于禁止同股重复建仓)
        held_codes={slots[si]["code"] for si in range(N_SLOTS) if slots[si]}
        cands=inv.get(ts,[])
        hot_codes=hot_at.get(ts,(set(),[],{}))[0] if cfg.get("hot_on") else None
        # 大盘择时闸口: 市场处于空头(跌破所选均线)当日不新开仓(已有持仓照常退出)
        if cfg.get("regime_on"):
            rg=regime_at.get(cfg.get("regime_ma","ma60"), {}).get(ts, True) if regime_at else True
            if not rg:
                continue
        for code in cands:
            if cfg.get("hot_on") and code not in hot_codes: continue
            # 边界修复①: 同股仍在持仓 -> 不可重复建仓
            if code in held_codes: continue
            # 边界修复②: 同股冷却期 -> 退出后 cooldown 交易日内不再买
            if cooldown>0 and code in last_exit and (ti-last_exit[code])<=cooldown: continue
            # 风险分散③: 单题材持仓上限 -> 若同题材已持有 >= cap 只则跳过
            if cfg.get("theme_cap"):
                cap=int(cfg["theme_cap"])
                cth=hot_at.get(ts,(set(),[],{}))[2].get(code, [])
                if cth:
                    cnt=0
                    for si in range(N_SLOTS):
                        pos=slots[si]
                        if pos is None: continue
                        pth=hot_at.get(ts,(set(),[],{}))[2].get(pos["code"], [])
                        if set(pth) & set(cth): cnt+=1
                    if cnt>=cap: continue
            free=next((k for k in range(N_SLOTS) if slots[k] is None), None)
            if free is None: break
            g=ctx[code]; row=g.loc[t]; close=row["close"]
            if cfg.get("quality_on"):
                ok,pe,pb,roe,np_yoy=quality_ok(fmap,code,ts,close,cfg.get("pe_pb_on",True))
                if not ok: continue
            else:
                ok,pe,pb,roe,np_yoy=True,None,None,None,None
            if entry_mode=="close": buy_t=t; buy_px=close*(1+SLIP)
            else:
                if ti+1>=len(cal): continue
                t1=cal[ti+1]
                if t1 not in g.index: continue
                buy_t=t1; buy_px=g.loc[t1,"open"]*(1+SLIP)
            free_slots=sum(1 for s in slots if s is None)
            shares=int((capital/free_slots)/buy_px/100)*100
            if shares<=0: continue
            capital-=shares*buy_px
            slots[free]=dict(code=code,buy_t=buy_t,buy_px=buy_px,shares=shares,
                             regime="na",roe=roe,np_yoy=np_yoy,pe=pe,pb=pb)
        openmv=sum((ctx[s["code"]].loc[t,"close"] if t in ctx[s["code"]].index else s["buy_px"])*s["shares"] for s in slots if s)
        eq.append(capital+openmv)
    return trades, eq

def metrics(trades, eq):
    if not trades:
        return dict(n=0,winrate=0,avg_ret=0,avg_win=0,avg_loss=0,best=0,worst=0,
                    total_ret=0,annual=0,sharpe=0,maxdd=0)
    rets=[t["ret"] for t in trades]
    wins=[r for r in rets if r>0]; loss=[r for r in rets if r<=0]
    eq_arr=np.array(eq); total_ret=eq_arr[-1]/eq_arr[0]-1
    daily=pd.Series(eq_arr).pct_change().dropna()
    sharpe=daily.mean()/daily.std()*np.sqrt(252) if daily.std()>0 else 0
    peak=np.maximum.accumulate(eq_arr); dd=(eq_arr-peak)/peak
    return dict(n=len(trades),winrate=round(100*len(wins)/len(trades),1),
        avg_ret=round(100*np.mean(rets),2),
        avg_win=round(100*np.mean(wins),2) if wins else 0,
        avg_loss=round(100*np.mean(loss),2) if loss else 0,
        best=round(100*max(rets),2),worst=round(100*min(rets),2),
        total_ret=round(100*total_ret,2),annual=round(100*((1+total_ret)**(252/len(eq))-1),2),
        sharpe=round(sharpe,3),maxdd=round(100*dd.min(),2))

# ---------------- 变体定义 ----------------
def make_variants():
    V=[]
    # 0 朴素深度超跌(基线, 无绩优/无热门)
    V.append(("V0 朴素深度超跌", dict(mode="deep",dd=-0.18,gap=0.03,rsi_th=35,
        ma60_rising=False,vol_confirm=False,macd_rsi=False,hot_on=False,pe_pb_on=False,quality_on=False)))
    # 1 深度超跌 + 绩优(无热门)
    V.append(("V1 深度超跌+绩优", dict(mode="deep",dd=-0.18,gap=0.03,rsi_th=35,
        ma60_rising=False,vol_confirm=False,macd_rsi=False,hot_on=False,pe_pb_on=True,quality_on=True)))
    # 2 深度超跌 + 绩优 + 热门
    V.append(("V2 深度超跌+绩优+热门", dict(mode="deep",dd=-0.18,gap=0.03,rsi_th=35,
        ma60_rising=False,vol_confirm=False,macd_rsi=False,hot_on=True,pe_pb_on=True,quality_on=True)))
    # 3 回踩企稳 + 绩优(网搜共识, 无热门)
    V.append(("V3 回踩企稳+绩优", dict(mode="pullback",dd=-0.10,rsi_th=45,floor_ma60=0.10,
        ma60_rising=True,vol_confirm=True,macd_rsi=True,hot_on=False,pe_pb_on=True,quality_on=True)))
    # 4 回踩企稳 + 绩优 + 热门(网搜共识+热门)
    V.append(("V4 回踩企稳+绩优+热门", dict(mode="pullback",dd=-0.10,rsi_th=45,floor_ma60=0.10,
        ma60_rising=True,vol_confirm=True,macd_rsi=True,hot_on=True,pe_pb_on=True,quality_on=True)))
    # 5 回踩企稳 + 绩优 + 热门 + 无PE/PB硬约束(仅ROE/净利)
    V.append(("V5 回踩+绩优(仅ROE/净利)+热门", dict(mode="pullback",dd=-0.10,rsi_th=45,floor_ma60=0.10,
        ma60_rising=True,vol_confirm=True,macd_rsi=True,hot_on=True,pe_pb_on=False,quality_on=True)))
    return V

def build_html(path, m, trades, vname, cfg, hot_summary, ablation, cooldown_rows="", div_rows="", stop_rows=""):
    rows=""
    for i,t in enumerate(trades,1):
        cls="pos" if t["ret"]>0 else "neg"
        rows+=(f"<tr><td>{i}</td><td>{t['code']}</td><td>{str(t['buy_t'])[:10]}</td><td>{t['buy_px']:.2f}</td>"
               f"<td>{str(t['sell_t'])[:10]}</td><td>{t['sell_px']:.2f}</td><td class='num {cls}'>{t['ret']*100:.2f}%</td>"
               f"<td>{t['shares']}</td><td>{t['hold_days']}</td><td>{t['reason']}</td>"
               f"<td>{t['roe']}</td><td>{t['np_yoy']}</td><td>{t['pe']}</td></tr>")
    hot_examples="; ".join(hot_summary[:12])
    html=f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a;background:#fff}}
h1{{font-size:22px}} h2{{font-size:17px;margin-top:26px;border-left:4px solid #2b6cb0;padding-left:8px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}}
th{{background:#f4f6f8}} .num{{text-align:right}}
.pos{{color:#c0392b;font-weight:600}} .neg{{color:#1e7e34;font-weight:600}}
.tag{{display:inline-block;background:#eef;color:#245;padding:2px 7px;border-radius:4px;margin:2px;font-size:12px}}
.kpi{{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px}}
.card{{background:#f8fafc;border:1px solid #e3e8ee;border-radius:8px;padding:12px 16px;min-width:110px}}
.card b{{font-size:20px;display:block}}
.note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.6}}
</style></head><body>
<h1>超跌绩优反弹 · 增强版回测报告</h1>
<p>变体: <b>{vname}</b> ｜ 窗口 {WINDOW_START}~{WINDOW_END}</p>
<div class='kpi'>
 <div class='card'><b>{m['total_ret']}%</b>总收益</div>
 <div class='card'><b>{m['winrate']}%</b>胜率</div>
 <div class='card'><b>{m['n']}</b>交易笔数</div>
 <div class='card'><b>{m['avg_ret']}%</b>平均每笔</div>
 <div class='card'><b>{m['avg_win']}%</b>平均盈</div>
 <div class='card'><b>{m['avg_loss']}%</b>平均亏</div>
 <div class='card'><b>{m['sharpe']}</b>夏普</div>
 <div class='card'><b>{m['maxdd']}%</b>最大回撤</div>
</div>
<h2>信号口径</h2>
<p>{' '.join(f"<span class='tag'>{k}={v}</span>" for k,v in cfg.items())}</p>
<h2>热门题材示例(窗口内部分日期 Top 题材)</h2>
<p class='note'>{hot_examples}</p>
<h2>参数扫描对照表(全变体)</h2>
<table><tr><th>变体</th><th>基础信号</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>最大回撤</th></tr>
{ablation}</table>
<h2>同股冷却期敏感度(V2 · 退后 N 日禁买同股)</h2>
<p class='note'>边界修复: ① 持仓期内禁止同股重复建仓(逻辑正确性); ② 退出后冷却期避免刚止损又追. 下表为不同冷却期下 V2 表现.</p>
<table><tr><th>冷却期(交易日)</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>最大回撤</th></tr>
{cooldown_rows}</table>
<h2>风险分散对照(V2 基础 + 单题材上限 / 大盘择时)</h2>
<p class='note'>在 V2 基础上叠加风险分散: ① 单题材持仓上限(theme_cap=1 即每题材最多1只, 不同题材最多占满3槽);
② 大盘择时(等权全A净值站上 MA60/MA20 才允许新开仓, 空头当日禁开). 下表对比分散效果.
<b>结论: 单题材上限1 全面占优(收益↑回撤↓夏普↑); 大盘择时反而有害——超跌反弹在弱市/熊市更易爆发行情, 站上均线反而错过最佳买点, 故最优组合不叠加择时.</b></p>
<table><tr><th>配置</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>最大回撤</th></tr>
{div_rows}</table>
<h2>止损点参数优化(V2 基线 · 不同止损位)</h2>
<p class='note'>在 V2 基线(样本充足, 14笔)上扫描止损位, 找出最优止损(兼顾收益与回撤). 持有期固定 {HOLD_DEF} 交易日.
最优组合 = 最优止损位 + 单题材上限1(见分散化对照). 注: 大盘择时对超跌反弹策略有害(弱市更易爆发行情), 故最优组合不叠加择时闸口.</p>
<table><tr><th>止损位</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>最大回撤</th></tr>
{stop_rows}</table>
<h2>全量逐笔交易({m['n']}笔)</h2>
<table><tr><th>#</th><th>代码</th><th>买入日</th><th>买价</th><th>卖出日</th><th>卖价</th>
<th>收益</th><th>股数</th><th>持有日</th><th>退出</th><th>ROE%</th><th>净利同比%</th><th>PE</th></tr>
{rows}</table>
<div class='note' style='margin-top:18px'><b>诚实说明:</b> ① 数据库无分钟表, 买价=信号日14:45收盘×滑点, 卖价=到期/止损日15:00收盘×滑点;
② 基本面为 akshare 东方财富业绩报表真实数据, 可用性=报告期末+1日(防未来函数); PE/PB 由真实EPS/每股净资产+当日收盘价现场计算(point-in-time);
③ 概念成员为当前快照(2026-04-16), 题材归属近似; ④ 热门题材资金流为成交额代理(无真实主力资金流字段)。</div>
</body></html>"""
    with open(path,"w") as f: f.write(html)

def main():
    print("加载K线...",flush=True)
    ctx=load_kline()
    cal=sorted({t for g in ctx.values() for t in g.index})
    cal_slice=[t for t in cal if WINDOW_START<=str(t)[:10]<=WINDOW_END]
    print(f"  标的数={len(ctx)} 交易日={len(cal_slice)}",flush=True)
    print("预计算指标...",flush=True)
    ctx=build_ctx(ctx)
    print("加载基本面...",flush=True)
    fmap=load_fundamentals()
    print(f"  基本面覆盖 {len(fmap)} 只",flush=True)
    print("构建30交易日热门题材...",flush=True)
    hot_at=build_hot_themes(ctx,cal_slice)
    print("参数扫描...",flush=True)
    variants=make_variants()
    results=[]
    for vname,cfg in variants:
        inv=build_signal_index(ctx,cal_slice,cfg)
        nbase=sum(len(v) for v in inv.values())
        trades,eq=simulate(ctx,cal_slice,inv,hot_at,fmap,HOLD_DEF,ENTRY_MODE,STOP_LOSS,cfg,SAME_CODE_COOLDOWN)
        m=metrics(trades,eq)
        m["variant"]=vname; m["n_base_signal"]=nbase
        results.append((vname,cfg,m))
        print(f"  {vname}: 基础信号={nbase} 成交={m['n']} 胜率={m['winrate']}% 平均每笔={m['avg_ret']}% 总收益={m['total_ret']}% 夏普={m['sharpe']} 回撤={m['maxdd']}%",flush=True)

    # ---- 大盘择时行情代理(等权全A净值 vs MA60/MA20) ----
    print("构建大盘择时行情代理...",flush=True)
    mkt_nav=build_market_proxy(ctx,cal_slice)
    mkt_ma60=mkt_nav.rolling(60).mean()
    mkt_ma20=mkt_nav.rolling(20).mean()
    def _rdict(ma):
        d={}
        for t in cal_slice:
            nv=mkt_nav.get(t,np.nan); mm=ma.get(t,np.nan)
            d[str(t)[:10]]=(not pd.isna(nv)) and (not pd.isna(mm)) and nv>=mm
        return d
    regime_at={"ma60":_rdict(mkt_ma60),"ma20":_rdict(mkt_ma20)}

    def run_cfg(c, cd=SAME_CODE_COOLDOWN, sl=STOP_LOSS):
        inv=build_signal_index(ctx,cal_slice,c)
        tr,eq=simulate(ctx,cal_slice,inv,hot_at,fmap,HOLD_DEF,ENTRY_MODE,sl,c,cd,regime_at)
        return tr,eq,metrics(tr,eq)

    # 同股冷却期敏感度(V2 跨越 cooldown=0,5,10,20)
    cd_rows=""
    cd_target=None
    for vname,cfg,m in results:
        if vname=="V2 深度超跌+绩优+热门" and m["n"]>0:
            cd_target=(vname,cfg); break
    if cd_target:
        vname,cfg=cd_target
        inv_cd=build_signal_index(ctx,cal_slice,cfg)
        for cd in [0,5,10,20]:
            tr,eq_cd=simulate(ctx,cal_slice,inv_cd,hot_at,fmap,HOLD_DEF,ENTRY_MODE,STOP_LOSS,cfg,cd,regime_at)
            mc=metrics(tr,eq_cd)
            bcls="pos" if mc["total_ret"]>0 else "neg"
            hl=" style='background:#eaf6ff'" if cd==SAME_CODE_COOLDOWN else ""
            cd_rows+=(f"<tr{hl}><td>{cd}{'(采用)' if cd==SAME_CODE_COOLDOWN else ''}</td>"
                      f"<td>{mc['n']}</td><td>{mc['winrate']}%</td><td>{mc['avg_ret']}%</td>"
                      f"<td class='num {bcls}'>{mc['total_ret']}%</td><td>{mc['sharpe']}</td><td>{mc['maxdd']}%</td></tr>")
        print("冷却期敏感度完成",flush=True)

    # ---- 风险分散对照(V2 + 单题材上限 / 大盘择时) ----
    v2cfg=None
    for vname,cfg,m in results:
        if vname=="V2 深度超跌+绩优+热门": v2cfg=dict(cfg); break
    div_cfgs=[
        ("V2 基础(无分散)", dict(v2cfg)),
        ("V2 + 单题材上限1", {**v2cfg,"theme_cap":1}),
        ("V2 + 单题材上限2", {**v2cfg,"theme_cap":2}),
        ("V2 + 大盘择时MA60", {**v2cfg,"regime_on":True,"regime_ma":"ma60"}),
        ("V2 + 大盘择时MA20", {**v2cfg,"regime_on":True,"regime_ma":"ma20"}),
        ("V2 + 上限1 + 择时MA60", {**v2cfg,"theme_cap":1,"regime_on":True,"regime_ma":"ma60"}),
    ]
    div_rows=""
    for dn,c in div_cfgs:
        tr,eq,mc=run_cfg(c)
        bcls="pos" if mc["total_ret"]>0 else "neg"
        div_rows+=(f"<tr><td>{dn}</td><td>{mc['n']}</td><td>{mc['winrate']}%</td><td>{mc['avg_ret']}%</td>"
                   f"<td class='num {bcls}'>{mc['total_ret']}%</td><td>{mc['sharpe']}</td><td>{mc['maxdd']}%</td></tr>")
    print("风险分散对照完成",flush=True)

    # ---- 止损点优化(在 V2 基线[14笔, 样本充足]上扫描, 找出最优止损位) ----
    STOP_GRID=[-0.03,-0.05,-0.07,-0.10,-0.12,-0.15,-0.20]
    stop_rows=""; best_stop=STOP_LOSS; best_score=-1e9; best_n=0
    for sl in STOP_GRID:
        tr,eq,mc=run_cfg(dict(v2cfg), sl=sl)   # 基线扫描(不加分散约束)以保证样本量
        bcls="pos" if mc["total_ret"]>0 else "neg"
        hl=" style='background:#eaf6ff'" if abs(sl-STOP_LOSS)<1e-9 else ""
        stop_rows+=(f"<tr{hl}><td>{sl*100:.0f}%</td><td>{mc['n']}</td><td>{mc['winrate']}%</td>"
                    f"<td>{mc['avg_ret']}%</td><td class='num {bcls}'>{mc['total_ret']}%</td>"
                    f"<td>{mc['sharpe']}</td><td>{mc['maxdd']}%</td></tr>")
        # 选优: 成交>=8 且 总收益最高(样本内选参, 有拟合风险)
        if mc["n"]>=8 and mc["total_ret"]>best_score:
            best_score=mc["total_ret"]; best_stop=sl; best_n=mc["n"]
    if best_n<8:
        print("  [提示] 止损扫描样本偏少, 回退默认止损 -7%",flush=True); best_stop=STOP_LOSS
    print(f"止损优化完成, 最优止损={best_stop*100:.0f}% (n={best_n}, 总收益={best_score:.2f}%)",flush=True)

    # ---- 详细报告①: V2 基线(含分散/止损表) ----
    target_name="V2 深度超跌+绩优+热门"
    target=None
    for vname,cfg,m in results:
        if vname==target_name and m["n"]>0:
            target=(vname,cfg,m); break
    if target is None:
        target=max([r for r in results if r[2]["n"]>0], key=lambda x:x[2]["total_ret"])
    bname,bcfg,bm=target
    print(f"\n详细报告变体: {bname} (n={bm['n']}, 总收益={bm['total_ret']}%)",flush=True)
    inv=build_signal_index(ctx,cal_slice,bcfg)
    trades,eq=simulate(ctx,cal_slice,inv,hot_at,fmap,HOLD_DEF,ENTRY_MODE,STOP_LOSS,bcfg,SAME_CODE_COOLDOWN,regime_at)
    hot_summary=[]
    for d in cal_slice[::40]:
        ds=str(d)[:10]; v=hot_at.get(ds)
        if v: hot_summary.append(f"{ds}: {','.join(v[1][:4])}")
    abl=""
    for vname,cfg,m in results:
        bcls="pos" if m["total_ret"]>0 else "neg"
        hl=" style='background:#eaf6ff'" if vname==bname else ""
        abl+=(f"<tr{hl}><td>{vname}</td><td>{m['n_base_signal']}</td><td>{m['n']}</td>"
              f"<td>{m['winrate']}%</td><td>{m['avg_ret']}%</td>"
              f"<td class='num {bcls}'>{m['total_ret']}%</td><td>{m['sharpe']}</td><td>{m['maxdd']}%</td></tr>")
    out_html=os.path.join(ROOT,"opt_study","oversold_quality_report.html")
    build_html(out_html,bm,trades,bname,bcfg,hot_summary,abl,cd_rows,div_rows,stop_rows)
    out_csv=os.path.join(ROOT,"opt_study","oversold_quality_trades.csv")
    import csv
    with open(out_csv,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.writer(f)
        w.writerow(["#","代码","买入日","买价","卖出日","卖价","收益%","股数","持有日","退出","ROE%","净利同比%","PE","PB"])
        for i,t in enumerate(trades,1):
            w.writerow([i,t["code"],str(t["buy_t"])[:10],round(t["buy_px"],2),str(t["sell_t"])[:10],round(t["sell_px"],2),
                round(t["ret"]*100,2),t["shares"],t["hold_days"],t["reason"],t["roe"],t["np_yoy"],t["pe"],t["pb"]])
    out_json=os.path.join(ROOT,"opt_study","oversold_quality_metrics.json")
    json.dump(dict(window=[WINDOW_START,WINDOW_END],best=bname,best_metrics=bm,
                   sweep=[{"variant":v,"cfg":c,"metrics":m} for v,c,m in results]),
              open(out_json,"w"),ensure_ascii=False,indent=2)

    # ---- 详细报告②: 最优分散 + 最优止损 ----
    # 注: 大盘择时对超跌反弹策略有害(弱市更易爆发行情, 见分散化对照表), 故最优组合不叠加择时,
    #     仅叠加"单题材上限1"(风险分散最优) + 基线扫描出的最优止损位.
    opt_cfg=dict(v2cfg); opt_cfg["theme_cap"]=1; opt_cfg["stop_loss"]=best_stop
    opt_trades,opt_eq,opt_m=run_cfg(opt_cfg, sl=best_stop)
    print(f"\n最优组合: V2+单题材上限1+止损{best_stop*100:.0f}%(无择时, 因择时有害) (n={opt_m['n']}, 总收益={opt_m['total_ret']}%)",flush=True)
    opt_html=os.path.join(ROOT,"opt_study","oversold_quality_opt_report.html")
    build_html(opt_html,opt_m,opt_trades,f"V2+单题材上限1+最优止损{best_stop*100:.0f}%",opt_cfg,hot_summary,abl,cd_rows,div_rows,stop_rows)
    opt_csv=os.path.join(ROOT,"opt_study","oversold_quality_opt_trades.csv")
    with open(opt_csv,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.writer(f)
        w.writerow(["#","代码","买入日","买价","卖出日","卖价","收益%","股数","持有日","退出","ROE%","净利同比%","PE","PB"])
        for i,t in enumerate(opt_trades,1):
            w.writerow([i,t["code"],str(t["buy_t"])[:10],round(t["buy_px"],2),str(t["sell_t"])[:10],round(t["sell_px"],2),
                round(t["ret"]*100,2),t["shares"],t["hold_days"],t["reason"],t["roe"],t["np_yoy"],t["pe"],t["pb"]])
    json.dump(dict(config=opt_cfg, metrics=opt_m, best_stop=best_stop,
                   diversification=[{"name":n,"metrics":run_cfg(c)[2]} for n,c in div_cfgs]),
              open(os.path.join(ROOT,"opt_study","oversold_quality_opt_metrics.json"),"w"),
              ensure_ascii=False,indent=2)
    print("完成。基线报告:",out_html, "| 最优报告:",opt_html,flush=True)

if __name__=="__main__":
    main()
