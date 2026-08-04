# -*- coding: utf-8 -*-
"""
C 尾盘偷袭板日度扫描 (14:44 与动量策略并行)
输出: 偷袭板选股 → picks_tracking.json → Bark
"""
import json, logging, os, sys
from datetime import datetime
from pathlib import Path

_PROJ = Path(__file__).resolve().parent.parent
_sys_parent = str(_PROJ.parent)
if _sys_parent not in sys.path:
    sys.path.insert(0, _sys_parent)
try:
    import momentum as _m
except ImportError:
    import importlib.util
    _init_file = _PROJ / '__init__.py'
    _spec = importlib.util.spec_from_file_location('momentum', _init_file, submodule_search_locations=[str(_PROJ)])
    if _spec and _spec.loader:
        _mod = importlib.util.module_from_spec(_spec)
        sys.modules['momentum'] = _mod
        _spec.loader.exec_module(_mod)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('c_tail_scan')


# tracking / DB 同步统一走 tools/tracking_utils.add_picks (公共方法),
# 详见 low_quality_scan.py 说明。本模块只保留报告推送。


def bark_push(title: str, body: str):
    try:
        import requests
        key = os.environ.get('BARK_DEVICE_KEY', '').strip()
        if not key: return
        if key.startswith('http'):
            parts = key.rstrip('/').split('/')
            key = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else key)
        requests.post('https://api.day.app/push',
                      json={'device_key': key, 'title': title, 'body': body[:3800],
                            'group': 'Momentum'}, timeout=10)
    except Exception as e:
        logger.warning(f"[C-Tail] 推送失败: {e}")


def run():
    logger.info("=" * 50)
    logger.info("[C-Tail] 尾盘偷袭板扫描启动")
    # 尾部风险门禁: 系统性暴跌时暂停选股(不改动选股逻辑, 仅尾部保护)
    try:
        from tools.risk_gate import crash_guard
        from tools.tracking_utils import bark_notify
        _halt, _reason = crash_guard()
        if _halt:
            logger.warning("风险门禁触发, 暂停选股: %s", _reason)
            bark_notify("⛔ 风险门禁·暂停选股", _reason)
            return [], _reason
    except Exception as _e:
        logger.warning("风险门禁检查异常(放行): %s", _e)
    from momentum.core.strategy_c_tail import run_c_scan
    picks, hot, report, is_bull = run_c_scan()
    if picks:
        from momentum.tools.tracking_utils import add_picks
        from momentum.tools.position_sizing import build_portfolio, MAX_HOLDINGS
        sized = build_portfolio(picks, max_n=MAX_HOLDINGS, method='risk_parity')
        add_picks(sized, 'C_TAIL', 0.95, 1.10)
    print(report)
    title = '🎯 C·偷袭板' if picks else '🎯 C·偷袭板(空)'
    bark_push(title, report)
    logger.info("[C-Tail] 完成")
    return picks, report
