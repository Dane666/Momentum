# -*- coding: utf-8 -*-
"""探针: 确认沪深300(000300)日线可通过 momentum.data 加载/抓取并缓存。"""
import sys, importlib.util
from pathlib import Path

ROOT = Path('.').resolve()
sys.path.insert(0, str(ROOT.parent))
spec = importlib.util.spec_from_file_location('momentum', ROOT / '__init__.py',
                                              submodule_search_locations=[str(ROOT)])
m = importlib.util.module_from_spec(spec); sys.modules['momentum'] = m
spec.loader.exec_module(m)

from momentum.data import load_or_fetch_kline, fetch_kline_from_api, init_db
init_db()
df = load_or_fetch_kline('000300', fetch_kline_from_api, '2024-01-01')
if df is None or df.empty:
    print('FETCH_FAIL')
else:
    df = df.sort_values('trade_date')
    print('OK rows=', len(df))
    print('range', df['trade_date'].iloc[0], '->', df['trade_date'].iloc[-1])
    print(df.tail(3).to_string())
