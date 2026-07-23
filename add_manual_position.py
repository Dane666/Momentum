# -*- coding: utf-8 -*-
"""手动录入持仓 — Webhook 触发 → 补全名称/止盈止损 → 追加 picks_tracking.json → Bark 通知"""
import json
import logging
import os
import sys
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('add_manual')

FIXED_STOP_PCT = 0.05      # 止损 -5%
TAKE_PROFIT_PCT = 0.10     # 止盈 +10%

# 引导 momentum 包, 复用统一跟踪公共方法(任何策略选股都走 tracking_utils)
import sys as _sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parent
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))
try:
    import momentum as _m  # noqa: F401
except ImportError:
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        'momentum', _ROOT / '__init__.py',
        submodule_search_locations=[str(_ROOT)])
    _mod = _ilu.module_from_spec(_spec)
    _sys.modules['momentum'] = _mod
    _spec.loader.exec_module(_mod)
from momentum.tools.tracking_utils import add_picks, bark_notify


def _exchange_prefix(code: str) -> str:
    """根据代码前缀返回交易所前缀"""
    code = str(code).strip()
    if code.startswith(('60', '68')):
        return 'sh'
    return 'sz'


def fetch_stock_name(code: str) -> str:
    """通过 Sina 实时行情接口获取股票名称"""
    code = str(code).strip()
    prefix = _exchange_prefix(code)
    url = f"https://hq.sinajs.cn/list={prefix}{code}"
    headers = {'Referer': 'https://finance.sina.com.cn/'}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = 'gbk'
        text = r.text.strip()
        if '"' in text:
            fields = text.split('"')[1].split(',')
            name = fields[0].strip() if fields else ''
            if name:
                return name
    except Exception as e:
        logger.error(f"Failed to fetch stock name for {code}: {e}")
    return ''


def main():
    if len(sys.argv) < 3:
        logger.error("Usage: python add_manual_position.py <code> <price>")
        sys.exit(1)

    code = str(sys.argv[1]).strip()
    try:
        price = float(sys.argv[2])
    except ValueError:
        logger.error(f"Invalid price: {sys.argv[2]}")
        sys.exit(1)

    # 1. 补全股票名称
    name = fetch_stock_name(code)
    if not name:
        logger.error(f"Could not fetch stock name for {code}, aborting")
        sys.exit(1)

    # 2. 计算止盈止损价格（同步回测规则的固定比例）
    sl_price = round(price * (1 - FIXED_STOP_PCT), 2)
    tp_price = round(price * (1 + TAKE_PROFIT_PCT), 2)

    # 3. 构建记录
    today = datetime.now().strftime('%Y-%m-%d')
    entry = {
        "code": code,
        "name": name,
        "price": price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "type": "MANUAL",
        "status": "HOLDING"
    }

    # 4. 统一注册到 position-monitor 监控(公共方法, 自动写 picks_tracking.json + DB)
    add_picks([entry], 'MANUAL', 1 - FIXED_STOP_PCT, 1 + TAKE_PROFIT_PCT,
              date=today, status='HOLDING')
    logger.info(f"Added manual position: {code} {name} @ {price}, SL={sl_price}, TP={tp_price}")

    # 5. Bark 成功通知
    bark_notify(
        "📌 手动持仓已录入",
        f"{name}({code}) 已加入监控\n"
        f"买入价: {price}\n"
        f"止损价: {sl_price} (-{FIXED_STOP_PCT*100:.0f}%)\n"
        f"止盈价: {tp_price} (+{TAKE_PROFIT_PCT*100:.0f}%)\n"
        f"日期: {today}\n"
        f"状态: HOLDING (已持仓，直接开始监控)"
    )
    logger.info("Done.")


if __name__ == '__main__':
    main()
