# -*- coding: utf-8 -*-
"""统一持仓跟踪公共方法 — 任何策略选出的股票都经此注册到 position-monitor 监控。

设计目的:
  之前 low_quality_scan / c_tail_scan / leader_scan / add_manual_position 各自
  实现了一份 save_to_tracking + sync_to_db + bark_push(几乎逐字重复)。本模块把
  "选股 → data/picks_tracking.json + stock_picks 表 → 供 position_monitor 监控
  + Bark 推送" 收敛为单一公共入口, 以后新增策略只需:

      from momentum.tools.tracking_utils import add_picks
      add_picks(picks, 'MY_STRATEGY', sl_ratio=0.92, tp_ratio=1.12)

  即可被 position_monitor 统一监控(止盈/止损触发 Bark 告警), 无需各自造轮子。

公共 API:
  add_picks(picks, pick_type, sl_ratio=0.95, tp_ratio=1.10,
            date=None, status='WATCHING', track_file=None, db_path=None) -> int
      将选股追加到 data/picks_tracking.json(按 date+code+type 去重) 并同步
      stock_picks 表, 供 position_monitor 统一监控。返回新增条数。
  bark_notify(title, body)
      统一 Bark 推送(自动解析 device key, 含 http 前缀兼容)。
"""
import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('tracking_utils')

# momentum 包根目录(tools/tracking_utils.py -> parent=tools -> parent=momentum 根)
PROJ = Path(__file__).resolve().parent.parent
TRACK_FILE = str(PROJ / 'data' / 'picks_tracking.json')


def _db_path(db_path=None):
    return db_path or os.environ.get(
        'MOMENTUM_DB_PATH', str(PROJ / 'qlib_pro_v16.db'))


def bark_notify(title: str, body: str):
    """统一 Bark 推送(自动解析 device key, 兼容 https://api.day.app/<key>/ 形式)."""
    try:
        import requests
    except Exception as e:
        logger.warning("[tracking] requests 不可用, 跳过推送: %s", e)
        return
    key = os.environ.get('BARK_DEVICE_KEY', '').strip()
    if not key:
        logger.warning("[tracking] BARK_DEVICE_KEY 未配置, 跳过推送")
        return
    if key.startswith('http'):
        parts = key.rstrip('/').split('/')
        key = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else key)
    try:
        r = requests.post(
            'https://api.day.app/push',
            json={'device_key': key, 'title': title,
                  'body': body[:3800], 'group': 'Momentum'},
            timeout=10)
        if r.status_code == 200:
            logger.info("[tracking] Bark 推送成功: %s", title)
        else:
            logger.error("[tracking] Bark 失败: %s", r.text)
    except Exception as e:
        logger.error("[tracking] Bark 推送异常: %s", e)


def _sync_db(recs, db_path=None):
    """将新增记录同步到 stock_picks 表(按 date+code 去重)."""
    if not recs:
        return
    try:
        con = sqlite3.connect(_db_path(db_path))
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS stock_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, code TEXT, name TEXT,
            price REAL, status TEXT, sl_price REAL, tp_price REAL, type TEXT,
            exit_price REAL, pnl_pct REAL, trigger_type TEXT, trigger_time TEXT,
            pnl_ratio REAL, track_status TEXT DEFAULT 'TRACKING',
            track_count INTEGER DEFAULT 0, day1_pnl REAL, day2_pnl REAL,
            day3_pnl REAL, max_pnl_3d REAL DEFAULT 0.0)''')
        for r in recs:
            cur.execute('SELECT id FROM stock_picks WHERE date=? AND code=?',
                        (r['date'], r['code']))
            if cur.fetchone():
                continue
            cur.execute('''INSERT INTO stock_picks
                (date,code,name,price,status,sl_price,tp_price,type,
                 track_status,track_count,max_pnl_3d)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (r['date'], r['code'], r['name'], r['price'], r['status'],
                 r['sl_price'], r['tp_price'], r['type'],
                 'TRACKING', 0, 0.0))
        con.commit()
        con.close()
        logger.info("[tracking] DB 同步 %d 只", len(recs))
    except Exception as e:
        logger.warning("[tracking] DB 同步失败(不影响 json 监控): %s", e)


def add_picks(picks: list, pick_type: str, sl_ratio: float = 0.95,
              tp_ratio: float = 1.10, date: str = None,
              status: str = 'WATCHING', track_file: str = None,
              db_path: str = None) -> int:
    """将策略选股统一注册到 position-monitor 监控。

    Args:
        picks: 选股列表, 每项为 dict(至少含 code/name/price)。
        pick_type: 策略类型标识(LOW_QUALITY / C_TAIL / LEADER / MANUAL / ...)。
        sl_ratio / tp_ratio: 止损/止盈比例(相对买入价)。
        date: 选股日期(默认今天)。
        status: 监控初始状态(默认 WATCHING; 手动持仓用 HOLDING)。
        track_file / db_path: 测试或特殊部署可覆盖路径。
    Returns:
        新增条数(已存在的 date+code+type 组合自动跳过)。
    """
    if not picks:
        return 0
    track_file = track_file or TRACK_FILE
    date = date or datetime.now().strftime('%Y-%m-%d')

    try:
        tracking = (json.loads(Path(track_file).read_text(encoding='utf-8'))
                    if os.path.exists(track_file) else [])
    except Exception:
        tracking = []
    seen = {(p.get('date'), p.get('code'), p.get('type')) for p in tracking}
    new_recs = []
    for p in picks:
        code = str(p.get('code', '')).strip()
        if not code:
            continue
        try:
            price = float(p.get('price'))
        except (TypeError, ValueError):
            logger.warning("[tracking] 跳过无效价格: %s", p)
            continue
        key = (date, code, pick_type)
        if key in seen:
            continue
        rec = dict(date=date, code=code, name=p.get('name') or code,
                   price=round(price, 2),
                   sl_price=round(price * sl_ratio, 2),
                   tp_price=round(price * tp_ratio, 2),
                   status=p.get('status', status), type=pick_type)
        tracking.append(rec)
        new_recs.append(rec)
        seen.add(key)

    if new_recs:
        try:
            os.makedirs(os.path.dirname(track_file), exist_ok=True)
            Path(track_file).write_text(
                json.dumps(tracking, ensure_ascii=False, indent=2),
                encoding='utf-8')
            logger.info("[tracking] 追加 %d 只(%s) 到 %s",
                        len(new_recs), pick_type, track_file)
        except Exception as e:
            logger.error("[tracking] 写入 %s 失败: %s", track_file, e)
            return 0
        _sync_db(new_recs, db_path)
    return len(new_recs)
