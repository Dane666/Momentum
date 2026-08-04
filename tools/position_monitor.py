# -*- coding: utf-8 -*-
"""持仓监控 — TP/SL触发 + 盈亏计算 + Bark分流通知 + 状态回写"""
import json, logging, os
from datetime import datetime
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('monitor')
TRACK_FILE = 'data/picks_tracking.json'
try:
    import momentum.tools.tracking_utils as _tu
    TRACK_FILE = _tu.TRACK_FILE  # 统一用绝对路径, 与 expire_old_picks 同源
except Exception:
    pass

def load():
    try:
        with open(TRACK_FILE) as f: tracking = json.load(f)
        changed = False
        for p in tracking:
            if 'status' not in p:
                p['status'] = 'WATCHING'; changed = True
            if 'sl_price' not in p:
                p['sl_price'] = round(p['price'] * 0.95, 2); changed = True
            if 'tp_price' not in p:
                p['tp_price'] = round(p['price'] * 1.10, 2); changed = True
            if 'type' not in p:
                p['type'] = 'STRATEGY'; changed = True
        if changed:
            save(tracking)
            logger.info("Migrated old-format records")
        return tracking
    except: return []

def save(data):
    os.makedirs('data', exist_ok=True)
    with open(TRACK_FILE, 'w') as f: json.dump(data, f, ensure_ascii=False, indent=2)

def fetch_price(code):
    try:
        from momentum.data import load_or_fetch_kline, fetch_kline_from_api
        df = load_or_fetch_kline(str(code), fetch_kline_from_api)
        if df is not None and not df.empty: return float(df['close'].iloc[-1])
    except: pass
    return None

def notify(title, msg):
    try:
        from momentum.notify.bark import send_bark
        send_bark(title, msg)
    except: pass

def run():
    logger.info("[Monitor] scanning...")
    # 定时清理: 删除 N 天前的过期计划/观察记录(保护 MANUAL/HOLDING 真实持仓)
    try:
        from momentum.tools.tracking_utils import expire_old_picks
        ttl = int(os.environ.get('PICK_TTL_DAYS', '5'))
        n_exp = expire_old_picks(ttl_days=ttl)
        if n_exp:
            logger.info("[Monitor] 自动清理 %d 只过期记录(>%d天)", n_exp, ttl)
    except Exception as e:
        logger.warning("[Monitor] 过期清理失败(跳过): %s", e)
    tracking = load()
    updated = False; alerts = []
    for i, p in enumerate(tracking):
        if p.get('status') not in ('WATCHING', 'HOLDING', None): continue
        entry = p['price']; sl = p.get('sl_price', entry*0.95); tp = p.get('tp_price', entry*1.10)
        current = fetch_price(p['code'])
        if current is None: continue
        pnl = (current - entry) / entry * 100
        triggered = False; trigger_type = ''
        if current >= tp:
            tracking[i]['status'] = 'TRIGGERED'
            tracking[i]['exit_price'] = current
            tracking[i]['pnl_pct'] = round(pnl, 2)
            tracking[i]['trigger_type'] = 'TP'
            tracking[i]['trigger_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tracking[i]['pnl_ratio'] = round(pnl, 2)
            updated = True; triggered = True; trigger_type = 'TP'
        elif current <= sl:
            tracking[i]['status'] = 'TRIGGERED'
            tracking[i]['exit_price'] = current
            tracking[i]['pnl_pct'] = round(pnl, 2)
            tracking[i]['trigger_type'] = 'SL'
            tracking[i]['trigger_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tracking[i]['pnl_ratio'] = round(pnl, 2)
            updated = True; triggered = True; trigger_type = 'SL'

        if triggered:
            pick_type = p.get('type', 'STRATEGY')
            direction = '止盈' if trigger_type == 'TP' else '止损'
            # 压力位卖点上下文: 记录带 pressure 字段即表示以压力位为卖出目标
            pctx = ''
            if p.get('pressure') is not None:
                pctx = (f"\n买点(支撑)¥{p.get('support', p['price']):.2f}  "
                        f"卖点(压力)¥{p['pressure']:.2f}")
            if pick_type == 'MANUAL':
                vmark = '压力位卖出' if p.get('pressure') is not None else '止盈'
                line = (
                    f"⚠️ 【实际持仓警告】您的持仓 {p['name']}({p['code']})"
                    f" 已达{vmark}点！\n"
                    f"当前价: ¥{current:.2f}  实际盈亏: {pnl:+.1f}%{pctx}\n"
                    f"请速去券商手动操作！"
                )
            else:
                tlabel = {'LOW_QUALITY': '低位绩优', 'C_TAIL': 'C尾盘',
                          'LEADER': '龙头', 'STRATEGY': '策略'}.get(
                    pick_type, '策略')
                dmark = '压力位卖出' if (trigger_type == 'TP' and p.get('pressure') is not None) else direction
                line = (
                    f"📊 【{tlabel}模拟提示】观察股 {p['name']}({p['code']})"
                    f" 已触发{dmark}信号。\n"
                    f"当前价: ¥{current:.2f}  模拟盈亏: {pnl:+.1f}%{pctx}"
                )
            alerts.append(line)
        else:
            logger.info(f"  {p['code']} {p['name']} ¥{current:.2f} {pnl:+.1f}% SL=¥{sl:.2f}")

    # ---- 计划池(VP_*) 止盈目标提醒: 仅提醒"触及压力位", 不做止损(计划非实盘持仓) ----
    for i, p in enumerate(tracking):
        if p.get('status') != 'PLAN':
            continue
        if p.get('type') not in ('VP_BREAKOUT', 'VP_PULLBACK'):
            continue
        tp = p.get('tp_price')
        if not tp:
            continue
        if p.get('tp_notified'):
            continue
        current = fetch_price(p['code'])
        if current is None:
            continue
        if current >= tp:
            tracking[i]['tp_notified'] = True
            updated = True
            sup = p.get('support') or p.get('price')
            pres = p.get('pressure') or tp
            alerts.append(
                f"🎯 【价量计划·止盈目标达成】{p['name']}({p['code']})\n"
                f"已触及压力位 ¥{tp:.2f}！\n"
                f"买点(支撑): ¥{sup:.2f}  卖点(压力): ¥{pres:.2f}  当前: ¥{current:.2f}\n"
                f"可考虑逢高止盈(涨至压力位附近卖出)。")

    if updated:
        save(tracking)
        notify("💼 持仓提醒", "\n".join(alerts))
        logger.info(f"Saved {len(alerts)} triggers")
