# -*- coding: utf-8 -*-
"""
龙头策略日度扫描 (尾盘 14:44 与动量策略并行)
输出: 龙头选股 + 热门行业 → 存入跟踪缓存 → 推送 Bark 卡片
"""
import json, logging, os, sys
from datetime import datetime
from pathlib import Path

# Bootstrap momentum package (兼容直接运行和 Actions 环境)
_PROJ = Path(__file__).resolve().parent.parent
_sys_parent = str(_PROJ.parent)
if _sys_parent not in sys.path:
    sys.path.insert(0, _sys_parent)
try:
    import momentum as _m  # noqa: F401
except ImportError:
    import importlib.util
    _init_file = _PROJ / '__init__.py'
    _spec = importlib.util.spec_from_file_location('momentum', _init_file, submodule_search_locations=[str(_PROJ)])
    if _spec and _spec.loader:
        _mod = importlib.util.module_from_spec(_spec)
        sys.modules['momentum'] = _mod
        _spec.loader.exec_module(_mod)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('leader_scan')


# tracking / DB 同步统一走 tools/tracking_utils.add_picks (公共方法)。


def bark_push(title: str, body: str):
    """推送 Bark 消息."""
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
        logger.warning(f"[Leader] 推送失败: {e}")


def has_active_leaders() -> bool:
    """检查是否有未完成的龙头持仓 (WATCHING 状态)."""
    track_file = 'data/picks_tracking.json'
    try:
        if os.path.exists(track_file):
            with open(track_file, 'r', encoding='utf-8') as f:
                tracking = json.load(f)
            active = [p for p in tracking
                      if p.get('type') == 'LEADER'
                      and p.get('status') in ('WATCHING', 'HOLDING')]
            if active:
                codes = [p['code'] for p in active]
                logger.info(f"[Leader] 仍有活跃龙头持仓: {codes}, 跳过新增")
                return True
    except Exception as e:
        logger.warning(f"[Leader] 检查活跃持仓失败: {e}")
    return False


def run():
    """主入口: 龙头策略扫描 → 保存 → 推送."""
    logger.info("=" * 50)
    logger.info("[Leader] 龙头策略扫描启动")

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

    # 已有活跃龙头持仓 → 不重复选股, 发状态卡片
    if has_active_leaders():
        logger.info("[Leader] 已有活跃龙头, 等待持仓解决后再选新龙头")
        status = "[Leader] 当前龙头持仓仍在观察中, 等待TP/SL触发或持仓到期后自动轮换"
        print(status)
        from momentum.core.leader_strategy import format_report, fetch_market_data, calc_sector_heat, build_sector_map_quick
        try:
            from momentum.factors.market import calc_ma60_gate_open
            is_bull, msg, detail = calc_ma60_gate_open('000001')
        except Exception:
            is_bull, msg, detail = True, "跳过", {}
        # 只发热门行业快照, 不选新股
        df_real = fetch_market_data()
        if df_real is not None and not df_real.empty:
            code_sector_map = build_sector_map_quick(df_real)
            heat = calc_sector_heat(df_real, code_sector_map, 4)
            hot = sorted(heat.items(), key=lambda x: -x[1])[:8]
            rpt = format_report([], hot, is_bull, msg, detail)
            rpt += '\n\n📌 当前有活跃龙头持仓, 等待轮换中...'
            print(rpt)
            bark_push('🐉 龙头策略(等待中)', rpt)
        return [], status

    from momentum.core.leader_strategy import run_leader_scan
    leaders, report, is_bull = run_leader_scan()

    if leaders:
        from momentum.tools.tracking_utils import add_picks
        from momentum.tools.position_sizing import build_portfolio, MAX_HOLDINGS
        sized = build_portfolio(leaders, max_n=MAX_HOLDINGS, method='risk_parity')
        add_picks(sized, 'LEADER', 0.95, 1.10)

    print(report)
    title = '🐉 龙头策略选股' if leaders else '🐉 龙头策略'
    bark_push(title, report)
    logger.info("[Leader] 完成")
    return leaders, report
