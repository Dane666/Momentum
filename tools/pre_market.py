# -*- coding: utf-8 -*-
"""
盘前全景早报 + 开仓评分 (每日 08:30 推送)

四级评分: 外围(美股/A50/恒生) + 宏观(DXY/CNH/美债) + 政策快讯
≥3 积极 | 1-2 谨慎 | 0~-1 多看少动 | ≤-2 空仓
"""
import sys, os, logging, re
from datetime import datetime
from typing import Optional, Dict, List, Tuple
import requests, pandas as pd, yfinance as yf

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('pre_market')

OVERSEAS = {'^GSPC':'标普500','^IXIC':'纳斯达克','^DJI':'道琼斯','^HSI':'恒生指数','XINA50.NYB':'A50期指'}
TECH = {'NVDA':'英伟达','AAPL':'苹果','TSLA':'特斯拉','MSFT':'微软','AMD':'AMD',
        'MU':'镁光','WDC':'西部数据','STX':'希捷','SMCI':'超微电脑','AVGO':'博通','INTC':'英特尔','^SOX':'费城半导体'}
MACRO = {'CNH=X':'离岸人民币','DX-Y.NYB':'美元指数','^TNX':'美10Y国债'}

POLICY_POS = ['降准','降息','减税','产业扶持','回购','增持','暂停IPO','印花税','促消费']
POLICY_NEG = ['加息','监管问询','立案调查','地缘冲突','制裁','关税','反倾销','退市']

SCORE_CHECKS = [
    ('标普500', lambda d: d.get('^GSPC',{}).get('change_pct',0)>0.005, lambda d: d.get('^GSPC',{}).get('change_pct',0)<-0.008),
    ('纳斯达克',lambda d: d.get('^IXIC',{}).get('change_pct',0)>0.005, lambda d: d.get('^IXIC',{}).get('change_pct',0)<-0.015),
    ('A50期指', lambda d: d.get('XINA50.NYB',{}).get('change_pct',0)>0.003, lambda d: d.get('XINA50.NYB',{}).get('change_pct',0)<-0.005),
    ('恒生指数',lambda d: d.get('^HSI',{}).get('change_pct',0)>0.005, lambda d: d.get('^HSI',{}).get('change_pct',0)<-0.008),
    ('美元指数',lambda d: d.get('DX-Y.NYB',{}).get('change_pct',0)<-0.003, lambda d: d.get('DX-Y.NYB',{}).get('change_pct',0)>0.003),
    ('人民币',  lambda d: d.get('CNH=X',{}).get('change_pct',0)<-0.002, lambda d: d.get('CNH=X',{}).get('change_pct',0)>0.003),
    ('美债10Y', lambda d: d.get('^TNX',{}).get('change',0)<-0.03, lambda d: d.get('^TNX',{}).get('change',0)>0.03),
]


def fetch_ticker(tk: str) -> Optional[Dict]:
    try:
        t = yf.Ticker(tk); h = t.history(period='5d')
        if h.empty or len(h)<2: return None
        l,p = h.iloc[-1], h.iloc[-2]
        cg = float(l['Close'])-float(p['Close'])
        cgp = cg/float(p['Close']) if float(p['Close'])>0 else 0
        return {'ticker':tk,'close':float(l['Close']),'change':cg,'change_pct':cgp,'date':h.index[-1].strftime('%Y-%m-%d')}
    except: return None


def fetch_all() -> Dict:
    r = {}
    for tk,nm in {**OVERSEAS,**TECH,**MACRO}.items():
        d = fetch_ticker(tk)
        if d: d['name']=nm; r[tk]=d
    return r


def scan_policy() -> Tuple[int,List[str]]:
    hits=[]; score=0
    try:
        resp = requests.get('https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=8.4.6', timeout=8,
                           headers={'User-Agent':'Mozilla/5.0'})
        if resp.status_code==200:
            for item in resp.json().get('data',{}).get('roll_data',[])[:30]:
                t = item.get('title','')
                for kw in POLICY_POS:
                    if kw in t: hits.append(f'+{kw}'); score+=1
                for kw in POLICY_NEG:
                    if kw in t: hits.append(f'-{kw}'); score-=1
    except: pass
    return min(score,2), hits[:5]


def score_market(data: Dict) -> Tuple[int,str,List[str]]:
    total=0; details=[]
    # 致命判定
    sp = data.get('^GSPC',{}).get('change_pct',0) if isinstance(data.get('^GSPC'),dict) else 0
    a50 = data.get('XINA50.NYB',{}).get('change_pct',0) if isinstance(data.get('XINA50.NYB'),dict) else 0
    if sp<-0.02 and a50<-0.01:
        return -3,'🔴 美股大跌+A50大跌,今日不宜开新仓',[f'标普{sp*100:.1f}% A50{a50*100:.1f}%']
    # 逐项评分
    for nm,pos,neg in SCORE_CHECKS:
        if pos(data): total+=1; details.append(f'+{nm}')
        elif neg(data): total-=1; details.append(f'-{nm}')
    # 宏观同步
    dxy = data.get('DX-Y.NYB',{}).get('change_pct',0) if isinstance(data.get('DX-Y.NYB'),dict) else 0
    cnh = data.get('CNH=X',{}).get('change_pct',0) if isinstance(data.get('CNH=X'),dict) else 0
    tnx = data.get('^TNX',{}).get('change',0) if isinstance(data.get('^TNX'),dict) else 0
    if dxy>0.003 and cnh>0.003 and tnx>0.05: total-=1; details.append('-三杀')
    # 政策
    ps,ph = scan_policy(); total+=ps; details.extend(ph)
    # 判定
    total = max(-3,min(4,total))
    if total>=3: v='🟢 积极操作'
    elif total>=1: v='🟡 谨慎参与'
    elif total>=0: v='⚪ 多看少动'
    else: v='🔴 空仓休息'
    return total,v,details


def fmt(v:float,pct:bool=True)->str:
    s='+' if v>=0 else ''
    return f'{s}{v*100:.2f}%' if pct else f'{s}{v:.2f}'


def build_report(data:Dict)->str:
    now=datetime.now().strftime('%Y-%m-%d %H:%M')
    total,verdict,details=score_market(data)
    lines=[f'📊 盘前早报 | {now}',f'开仓评分: {total:+d} → {verdict}']
    if details: lines.append(f'明细: {",".join(details[:8])}')
    lines.append('─'*40)
    lines.append('🇺🇸 外围市场')
    for t,n in OVERSEAS.items():
        d=data.get(t)
        if d: lines.append(f'  {"🔺" if d["change_pct"]>0 else "🔻"} {n}: {d["close"]:.2f} ({fmt(d["change_pct"])})')
    lines.append('\n💻 核心科技')
    for t,n in TECH.items():
        d=data.get(t)
        if d: lines.append(f'  {"🔺" if d["change_pct"]>0 else "🔻"} {n}: {d["close"]:.2f} ({fmt(d["change_pct"])})')
    lines.append('\n🌍 宏观')
    for t,n in MACRO.items():
        d=data.get(t)
        if d:
            if t=='^TNX': lines.append(f'  📌 {n}: {d["close"]:.2f}% ({fmt(d["change"],False)}%)')
            elif t=='CNH=X': lines.append(f'  📌 {n}: {d["close"]:.4f} ({fmt(d["change_pct"])})')
            else: lines.append(f'  📌 {n}: {d["close"]:.2f} ({fmt(d["change_pct"])})')
    lines.append(f'\n📅 {list(data.values())[0]["date"] if data else "N/A"}')
    lines.append('⏰ 09:25 竞价扫描见')
    return '\n'.join(lines)


def send_feishu(text:str):
    from momentum import config as cfg
    u=getattr(cfg,'FEISHU_WEBHOOK_URL','').strip()
    if u: requests.post(u,json={'msg_type':'text','content':{'text':text}},timeout=10)


def run():
    logger.info('[PreMarket] fetching...')
    d=fetch_all()
    if not d: return logger.error('no data')
    rpt=build_report(d); print(rpt); send_feishu(rpt)


if __name__=='__main__': run()
