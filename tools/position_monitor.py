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

# ---- 增强退出规则(评估报告建议③): 移动止盈 / 分批止盈 / 到期减仓 ----
# 本系统为"监控+提醒"工具(不自动交易), 故增强项以更精细的 Bark 提醒 + 触发为准。
TRAIL_START = float(os.environ.get('MON_TRAIL_START', '0.05'))   # 盈利≥5% 启用移动止盈
TRAIL_PCT = float(os.environ.get('MON_TRAIL_PCT', '0.07'))       # 从高点回撤7%触发移动止损
SCALED_TP = [0.10, 0.20, 0.30]   # 分批止盈阈值(各提醒一次: 卖出1/3→1/3→余下)
HOLD_MAX_DAYS = int(os.environ.get('MON_HOLD_MAX_DAYS', '20'))   # 持仓超此天数提醒减仓/退出

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


def _days_held(date_str):
    """返回信号登记日到今天的自然日数; 无效日期返回 None。"""
    if not date_str:
        return None
    try:
        d0 = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
        return (datetime.now() - d0).days
    except Exception:
        return None


def _scaled_tp_line(p, thr, current, pnl):
    tier = {0.10: '第一批(1/3)', 0.20: '第二批(1/3)', 0.30: '第三批(余下)'}.get(thr, f'{int(thr*100)}%')
    nm = p.get('name') or p.get('code', '?')
    w_s = f"  建议仓位{p['weight']*100:.0f}%" if p.get('weight') else ""
    return (f"🪙 【分批止盈提醒】{nm}({p['code']}) 已达 +{int(thr*100)}% 盈利!\n"
            f"当前价 ¥{current:.2f}  浮动盈亏 {pnl:+.1f}%{w_s}\n"
            f"建议卖出{tier}锁定利润, 余下跟随移动止盈。")


def _expiry_line(p, days, current, pnl, cap=None):
    cap = cap or HOLD_MAX_DAYS
    nm = p.get('name') or p.get('code', '?')
    w_s = f"  建议仓位{p['weight']*100:.0f}%" if p.get('weight') else ""
    return (f"⏳ 【到期减仓提醒】{nm}({p['code']}) 已持仓 {days} 天(≥{cap})\n"
            f"当前价 ¥{current:.2f}  浮动盈亏 {pnl:+.1f}%{w_s}\n"
            f"已达持仓上限周期, 建议减仓/落袋, 释放资金等待新信号。")

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

        # ---- 增强退出规则(建议③) ----
        # 1) 跟踪最高收益
        max_pnl = max(p.get('max_pnl', pnl) or pnl, pnl)
        tracking[i]['max_pnl'] = round(max_pnl, 2)
        # 2) 移动止盈: 盈利≥TRAIL_START 启用, 止损线上移(只升不降, 至少保本)
        trail_stop = p.get('trail_stop')
        if pnl >= TRAIL_START * 100:
            new_stop = max(current * (1 - TRAIL_PCT), entry)
            if trail_stop is None or new_stop > trail_stop:
                tracking[i]['trail_stop'] = round(new_stop, 2)
                trail_stop = new_stop
        # 3) 分批止盈提醒(各阈值仅提醒一次)
        for t in SCALED_TP:
            key = f'tp_part_{int(t*100)}'
            if pnl >= t * 100 and not p.get(key):
                tracking[i][key] = True
                updated = True
                alerts.append(_scaled_tp_line(p, t, current, pnl))
        # 4) 到期减仓提醒(优先用每笔持仓自己的 hold_max_days, 否则全局 HOLD_MAX_DAYS)
        days_held = _days_held(p.get('date'))
        hold_cap = p.get('hold_max_days') or HOLD_MAX_DAYS
        if hold_cap and days_held is not None and days_held >= hold_cap \
                and not p.get('expiry_notified'):
            tracking[i]['expiry_notified'] = True
            updated = True
            alerts.append(_expiry_line(p, days_held, current, pnl, hold_cap))

        # ---- 触发判定: 移动止盈 > 固定止盈 > 固定止损 ----
        triggered = False; trigger_type = ''
        if trail_stop is not None and current <= trail_stop and pnl > 0:
            tracking[i]['status'] = 'TRIGGERED'
            tracking[i]['exit_price'] = current
            tracking[i]['pnl_pct'] = round(pnl, 2)
            tracking[i]['trigger_type'] = 'TRAIL'
            tracking[i]['trigger_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tracking[i]['pnl_ratio'] = round(pnl, 2)
            updated = True; triggered = True; trigger_type = 'TRAIL'
        elif current >= tp:
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
            direction = {'TP': '止盈', 'SL': '止损', 'TRAIL': '移动止盈'}.get(trigger_type, '止盈')
            # 压力位卖点上下文: 记录带 pressure 字段即表示以压力位为卖出目标
            pctx = ''
            if p.get('pressure') is not None:
                pctx = (f"\n买点(支撑)¥{p.get('support', p['price']):.2f}  "
                        f"卖点(压力)¥{p['pressure']:.2f}")
            if pick_type == 'MANUAL':
                vmark = ('压力位卖出' if (trigger_type == 'TP' and p.get('pressure') is not None)
                         else direction)
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
                dmark = ('压力位卖出' if (trigger_type == 'TP' and p.get('pressure') is not None)
                         else direction)
                line = (
                    f"📊 【{tlabel}模拟提示】观察股 {p['name']}({p['code']})"
                    f" 已触发{dmark}信号。\n"
                    f"当前价: ¥{current:.2f}  模拟盈亏: {pnl:+.1f}%{pctx}"
                )
            alerts.append(line)
        else:
            t_s = f"  trail_stop=¥{trail_stop:.2f}" if trail_stop else ""
            logger.info(f"  {p['code']} {p['name']} ¥{current:.2f} {pnl:+.1f}% "
                        f"SL=¥{sl:.2f}{t_s}")

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
