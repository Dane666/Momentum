# -*- coding: utf-8 -*-
"""
_universe.py —— 候选池质量/风控过滤(共享)

当前提供 ST/*ST 过滤: 从 harness 的 data/stock_names.json 识别名称含 "ST"/"*ST"
的股票, 在打分/回测前剔除。理由:
  - ST 股 5% 涨跌停(非模型假设的 10%/20%), 止损-8% / 突破判定失真;
  - 戴帽=财务/退市风险, 特质性风险, 横截面排序 alpha 不适用;
  - 流动性差。与"涨停/一字板过滤"同属候选池质量闸。

使用:
  from _universe import load_st_codes, filter_st
  pan = filter_st(pan)   # pan 须含零填充字符串列 'code'
"""
from pathlib import Path
import json


def load_st_codes():
    """返回 ST/*ST 股票的零填充 code 集合(名称含 'ST' 或 '*ST')。

    数据源: data/stock_names.json(harness 名称映射, 与 Bark 推送/持仓去重同源)。
    读取失败或文件缺失时返回空集(不阻断主流程)。
    """
    p = Path('data/stock_names.json')
    st = set()
    if not p.exists():
        return st
    try:
        d = json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return st
    items = d.items() if isinstance(d, dict) else \
        [(it.get('code'), it.get('name')) for it in d if isinstance(it, dict)]
    for code, name in items:
        if name and 'ST' in str(name).upper():
            st.add(str(code).zfill(6))
    return st


def filter_st(pan):
    """剔除 ST/*ST 股票。pan 须含 'code'(零填充字符串或可被 zfill)。

    返回过滤后的 DataFrame, 并打印丢弃数量。
    """
    st = load_st_codes()
    if not st:
        return pan
    before = len(pan)
    codes = pan['code'].astype(str).str.zfill(6)
    pan = pan[~codes.isin(st)].copy()
    dropped = before - len(pan)
    if dropped:
        print(f'[universe] ST/*ST 过滤丢弃 {dropped} 只戴帽风险票')
    return pan
