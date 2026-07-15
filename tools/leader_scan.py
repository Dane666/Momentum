# -*- coding: utf-8 -*-
"""
龙头策略日度扫描 (尾盘 14:44 与动量策略并行)
输出: 龙头选股 + 热门行业 → 存入跟踪缓存 → 推送 Bark 卡片
"""
import json, logging, os, sys
from datetime import datetime
from pathlib import Path

_PROJ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJ.parent))
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('leader_scan')


def save_picks_to_tracking(leaders: list):
    """推入龙头选股到 picks_tracking.json (type=LEADER)."""
    today = datetime.now().strftime('%Y-%m-%d')
    track_file = 'data/picks_tracking.json'
    tracking = []
    try:
        if os.path.exists(track_file):
            with open(track_file, 'r', encoding='utf-8') as f:
                tracking = json.load(f)
    except Exception as e:
        logger.warning(f"[Leader] 读取 picks_tracking.json 失败: {e}")
    if any(p.get('date') == today and p.get('type') == 'LEADER' for p in tracking):
        logger.info(f"[Leader] {today} 已有龙头记录, 跳过")
        return
    inserted = 0
    for l in leaders:
        tracking.append({'date': today, 'code': l['code'], 'name': l['name'],
                         'price': l['price'], 'sl_price': round(l['price'] * 0.95, 2),
                         'tp_price': round(l['price'] * 1.10, 2),
                         'status': 'WATCHING', 'type': 'LEADER'})
        inserted += 1
    try:
        os.makedirs('data', exist_ok=True)
        with open(track_file, 'w', encoding='utf-8') as f:
            json.dump(tracking, f, ensure_ascii=False, indent=2)
        logger.info(f"[Leader] 已保存 {inserted} 只龙头")
    except Exception as e:
        logger.error(f"[Leader] 写入失败: {e}")


def sync_to_db(leaders: list):
    """同步龙头选股到 stock_picks 表."""
    try:
        import sqlite3
        today = datetime.now().strftime('%Y-%m-%d')
        db_path = os.environ.get('MOMENTUM_DB_PATH', 'qlib_pro_v16.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS stock_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, code TEXT, name TEXT,
            price REAL, status TEXT, sl_price REAL, tp_price REAL, type TEXT,
            exit_price REAL, pnl_pct REAL, trigger_type TEXT, trigger_time TEXT,
            pnl_ratio REAL, track_status TEXT DEFAULT 'TRACKING',
            track_count INTEGER DEFAULT 0, day1_pnl REAL, day2_pnl REAL,
            day3_pnl REAL, max_pnl_3d REAL DEFAULT 0.0,
            sl_triggered INTEGER DEFAULT 0, sl_recovery REAL)''')
        for l in leaders:
            cursor.execute('SELECT id FROM stock_picks WHERE date=? AND code=?', (today, l['code']))
            if cursor.fetchone():
                continue
            sl = round(l['price'] * 0.95, 2); tp = round(l['price'] * 1.10, 2)
            cursor.execute('''INSERT INTO stock_picks
                (date,code,name,price,status,sl_price,tp_price,type,track_status,track_count,max_pnl_3d)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (today, l['code'], l['name'], l['price'], 'WATCHING', sl, tp,
                 'LEADER', 'TRACKING', 0, 0.0))
        conn.commit(); conn.close()
        logger.info(f"[Leader] DB同步 {len(leaders)} 只龙头")
    except Exception as e:
        logger.warning(f"[Leader] DB同步失败: {e}")


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
        save_picks_to_tracking(leaders)
        sync_to_db(leaders)

    print(report)
    title = '🐉 龙头策略选股' if leaders else '🐉 龙头策略'
    bark_push(title, report)
    logger.info("[Leader] 完成")
    return leaders, report
