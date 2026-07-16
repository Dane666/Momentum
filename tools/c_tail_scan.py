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


def save_to_tracking(picks: list):
    today = datetime.now().strftime('%Y-%m-%d')
    track_file = 'data/picks_tracking.json'
    tracking = []
    try:
        if os.path.exists(track_file):
            with open(track_file, 'r', encoding='utf-8') as f:
                tracking = json.load(f)
    except Exception:
        pass
    if any(p.get('date') == today and p.get('type') == 'C_TAIL' for p in tracking):
        return
    for l in picks:
        tracking.append({
            'date': today, 'code': l['code'], 'name': l['name'],
            'price': l['price'], 'sl_price': round(l['price'] * 0.95, 2),
            'tp_price': round(l['price'] * 1.10, 2),
            'status': 'WATCHING', 'type': 'C_TAIL',
        })
    try:
        os.makedirs('data', exist_ok=True)
        with open(track_file, 'w', encoding='utf-8') as f:
            json.dump(tracking, f, ensure_ascii=False, indent=2)
        logger.info(f"[C-Tail] 保存 {len(picks)} 只")
    except Exception as e:
        logger.error(f"[C-Tail] 写入失败: {e}")


def sync_to_db(picks: list):
    try:
        import sqlite3
        today = datetime.now().strftime('%Y-%m-%d')
        db_path = os.environ.get('MOMENTUM_DB_PATH', 'qlib_pro_v16.db')
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS stock_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, code TEXT, name TEXT,
            price REAL, status TEXT, sl_price REAL, tp_price REAL, type TEXT,
            exit_price REAL, pnl_pct REAL, trigger_type TEXT, trigger_time TEXT,
            pnl_ratio REAL, track_status TEXT DEFAULT 'TRACKING',
            track_count INTEGER DEFAULT 0, day1_pnl REAL, day2_pnl REAL,
            day3_pnl REAL, max_pnl_3d REAL DEFAULT 0.0,
            sl_triggered INTEGER DEFAULT 0, sl_recovery REAL)''')
        for l in picks:
            cur.execute('SELECT id FROM stock_picks WHERE date=? AND code=?', (today, l['code']))
            if cur.fetchone(): continue
            sl = round(l['price'] * 0.95, 2); tp = round(l['price'] * 1.10, 2)
            cur.execute('''INSERT INTO stock_picks
                (date,code,name,price,status,sl_price,tp_price,type,track_status,track_count,max_pnl_3d)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (today, l['code'], l['name'], l['price'], 'WATCHING', sl, tp,
                 'C_TAIL', 'TRACKING', 0, 0.0))
        conn.commit(); conn.close()
        logger.info(f"[C-Tail] DB 同步 {len(picks)} 只")
    except Exception as e:
        logger.warning(f"[C-Tail] DB 失败: {e}")


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
    from momentum.core.strategy_c_tail import run_c_scan
    picks, hot, report, is_bull = run_c_scan()
    if picks:
        save_to_tracking(picks)
        sync_to_db(picks)
    print(report)
    title = '🎯 C·偷袭板' if picks else '🎯 C·偷袭板(空)'
    bark_push(title, report)
    logger.info("[C-Tail] 完成")
    return picks, report
