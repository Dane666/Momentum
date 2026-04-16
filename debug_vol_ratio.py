#!/usr/bin/env python
"""调试量比筛选问题 - 直接用adata接口"""
import adata
import pandas as pd

# 获取实时行情 (使用adata原生接口)
df = adata.stock.market.list_market_current(fs='沪深A股')
print(f"获取到 {len(df)} 只股票")
print(f"列名: {df.columns.tolist()}")

# 找到涨幅和量比列
change_col = '涨跌幅' if '涨跌幅' in df.columns else 'change_pct'
vol_col = '量比' if '量比' in df.columns else 'volume_ratio'

df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
df[vol_col] = pd.to_numeric(df[vol_col], errors='coerce')

# 涨幅在4-9.2%区间的
candidates = df[(df[change_col] >= 4.0) & (df[change_col] <= 9.2)]
print(f'\n涨幅筛选后: {len(candidates)} 只')

# 检查量比分布
print(f'\n量比统计:')
print(candidates[vol_col].describe())

# 量比>=1.0的
q1 = candidates[candidates[vol_col] >= 1.0]
print(f'\n量比>=1.0: {len(q1)} 只')

# 量比>=1.2的
q2 = candidates[candidates[vol_col] >= 1.2]
print(f'量比>=1.2: {len(q2)} 只')

# 检查海通发展和云煤能源
code_col = '股票代码' if '股票代码' in df.columns else 'code'
name_col = '股票名称' if '股票名称' in df.columns else 'name'

print(f'\n=== 目标股票的量比 ===')
for code in ['603162', '600792']:
    row = df[df[code_col] == code]
    if not row.empty:
        name = row[name_col].values[0]
        vr = row[vol_col].values[0]
        chg = row[change_col].values[0]
        print(f'{code} {name}: 涨幅={chg:.2f}%, 量比={vr}')
    else:
        print(f'{code}: 未找到')

# 显示量比>=1.2且涨幅在区间内的股票
print(f'\n=== 量比>=1.2 且 涨幅4~9.2%的股票 (前20) ===')
if not q2.empty:
    q2_display = q2[[code_col, name_col, change_col, vol_col]].head(20)
    print(q2_display.to_string())
else:
    print("无")
