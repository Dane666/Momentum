# Momentum 数据源规范 v3

## 更新日志
- **2026-01-27 v3**: 统一使用腾讯接口获取K线，避免数据源切换导致格式不一致
- **2026-01-27 v2**: 统一数据获取接口，新浪为主数据源

## 设计原则

1. **单一数据源**: 每种数据类型只使用一个数据源，避免切换导致格式不一致
2. **缓存优先**: 所有K线数据优先从SQLite缓存读取，减少API请求
3. **禁用代理**: 全局禁用系统代理，确保爬取正常

## 数据源架构

### 实时行情
| 数据类型 | 主数据源 | 备用数据源 | 状态 |
|---------|---------|-----------|------|
| 沪深A股行情 | 新浪 | efinance | ✅ 稳定 |
| ETF行情 | 新浪 | efinance | ✅ 稳定 |
| 沪股通/深股通 | 新浪 | - | ✅ 稳定 |

### K线数据
| 数据类型 | 主数据源 | 备用数据源 | 状态 |
|---------|---------|-----------|------|
| 个股K线 | adata | 腾讯 | ✅ 稳定 |
| ETF K线 | 腾讯 | - | ✅ 稳定 |
| 指数K线 | 腾讯 | 东财 | ✅ 稳定 |

### 辅助数据
| 数据类型 | 数据源 | 状态 |
|---------|--------|------|
| 板块概念 | adata | ✅ 可用 |
| 资金流向 | efinance | ⚠️ 可能不稳定 |
| 北向资金 | adata | ✅ 稳定 |

## 公共接口

所有数据获取统一使用 `momentum/data/fetcher.py`:

```python
from momentum.data import (
    fetch_realtime_quotes,   # 实时行情 (新浪优先)
    fetch_kline_from_api,    # K线数据 (adata/腾讯)
    fetch_market_index,      # 指数K线 (腾讯)
    fetch_quotes_sina,       # 新浪行情 (底层接口)
    fetch_stock_concept,     # 板块概念
    fetch_etf_list,          # ETF列表
)
```

## 代理配置

全局禁用代理，确保数据爬取正常：

```python
from momentum.data import disable_proxy
disable_proxy()
```

或在 `main.py` 入口处：
```python
import os
PROXY_KEYS = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 
              'all_proxy', 'ALL_PROXY']
for key in PROXY_KEYS:
    os.environ.pop(key, None)
os.environ['no_proxy'] = '*'
```

## 注意事项

1. **efinance 不稳定**: 实时行情接口经常返回空数据或报错，已降级为备用
2. **新浪限制**: 单次请求最多 80 只股票，已自动分块处理
3. **腾讯K线**: 最多返回 500 天历史数据，足够回测使用
