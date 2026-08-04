# -*- coding: utf-8 -*-
"""手动录入持仓 — Webhook 触发 → 补全名称/止盈止损 → 追加 picks_tracking.json → Bark 通知

两种录入方式:
  1) 普通持仓(默认): 固定止损 -5% / 止盈 +10% (与回测默认规则一致)
  2) 价量计划池持仓(--vp): 从 data/volume_price_plan.json 读取该股"买点(支撑)/卖点(压力)",
     以 压力位 作为实盘止盈目标(对应回测已验证的"触及压力位附近卖出"卖点),
     止损锚定支撑位(突破 -8% / 缩量回踩 -5%)。登记后自动清除对应的 PLAN 计划池记录,
     避免"计划提醒"与"实盘卖出提醒"重复推送。

监控端 position_monitor 在价格触及 压力位 时会推送"⚠️ 实际持仓·压力位卖出"提醒。
"""
import argparse
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
# 本脚本位于仓库根目录, data/ 为其子目录, 故 PROJ = 脚本所在目录(= 仓库根)
_ROOT = os.path.dirname(os.path.abspath(__file__))
PROJ = _ROOT
if PROJ not in sys.path:
    sys.path.insert(0, PROJ)
try:
    import momentum as _m  # noqa: F401
except ImportError:
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        'momentum', os.path.join(PROJ, '__init__.py'),
        submodule_search_locations=[PROJ])
    _mod = _ilu.module_from_spec(_spec)
    sys.modules['momentum'] = _mod
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


def _lookup_vp_plan(code: str):
    """从价量计划池 JSON 读取该股的 买点(支撑)/卖点(压力)/信号类型。"""
    p = os.path.join(PROJ, 'data', 'volume_price_plan.json')
    if not os.path.exists(p):
        return None
    try:
        d = json.loads(open(p, encoding='utf-8').read())
        for pk in d.get('picks', []):
            if str(pk.get('code')) == str(code):
                return pk
    except Exception as e:
        logger.warning(f"[vp] 读取计划池失败: {e}")
    return None


def _remove_plan_record(code: str):
    """登记真实持仓后, 清除对应的 PLAN 计划池记录(避免重复提醒)。"""
    tf = os.path.join(PROJ, 'data', 'picks_tracking.json')
    if not os.path.exists(tf):
        return
    try:
        arr = json.loads(open(tf, encoding='utf-8').read())
        kept = [x for x in arr
                if not (str(x.get('code')) == str(code)
                        and x.get('status') == 'PLAN'
                        and x.get('type') in ('VP_BREAKOUT', 'VP_PULLBACK'))]
        if len(kept) != len(arr):
            json.dump(kept, open(tf, 'w', encoding='utf-8'),
                      ensure_ascii=False, indent=2)
            logger.info(f"[vp] 已清除 PLAN 计划池记录(代码 {code}), "
                        f"避免与实盘卖出提醒重复")
    except Exception as e:
        logger.warning(f"[vp] 清除 PLAN 记录失败(忽略): {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("code")
    ap.add_argument("price", type=float)
    ap.add_argument("--vp", action="store_true",
                    help="价量计划池模式: 用计划池的支撑/压力位作为止损/止盈(压力位卖出)")
    ap.add_argument("--support", type=float, default=None,
                    help="显式指定支撑位(买点), 覆盖/补充计划池")
    ap.add_argument("--pressure", type=float, default=None,
                    help="显式指定压力位(卖点), 覆盖/补充计划池")
    a = ap.parse_args()

    code = str(a.code).strip()
    price = a.price

    # 1. 补全股票名称
    name = fetch_stock_name(code)
    if not name:
        logger.error(f"Could not fetch stock name for {code}, aborting")
        sys.exit(1)

    # 2. 解析支撑/压力位(价量计划池模式)
    support, pressure, kind = a.support, a.pressure, None
    if a.vp or support is None or pressure is None:
        lp = _lookup_vp_plan(code)
        if lp:
            kind = lp.get('kind')
            support = support if support is not None else lp.get('support')
            pressure = pressure if pressure is not None else lp.get('pressure')
    vp_mode = support is not None and pressure is not None

    if a.vp and not vp_mode:
        logger.error("未找到该股的价量计划池记录(支撑/压力位); "
                     "请先跑 volume_price_scan 或手动 --support/--pressure")
        sys.exit(1)

    # 3. 计算止盈/止损
    if vp_mode:
        sl_ratio = 0.92 if kind == 'breakout' else 0.95  # 突破-8% / 回踩-5%
        sl_price = round(support * sl_ratio, 2)
        tp_price = round(pressure, 2)
        note = (f"压力位卖出参考: 买(支撑)¥{support} 卖(压力)¥{pressure}"
                f"({'突破放量' if kind=='breakout' else '缩量回踩'})")
    else:
        sl_price = round(price * (1 - FIXED_STOP_PCT), 2)
        tp_price = round(price * (1 + TAKE_PROFIT_PCT), 2)
        note = f"固定规则: 止损-{FIXED_STOP_PCT*100:.0f}% / 止盈+{TAKE_PROFIT_PCT*100:.0f}%"
    today = datetime.now().strftime('%Y-%m-%d')

    # 4. 构建记录(含 support/pressure 供监控端给出压力位卖出提醒)
    entry = {
        "code": code,
        "name": name,
        "price": price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "type": "MANUAL",
        "status": "HOLDING",
    }
    if vp_mode:
        entry["support"] = round(support, 2)
        entry["pressure"] = round(pressure, 2)
        entry["vp_exit"] = "pressure"

    # 5. 统一注册到 position-monitor 监控(公共方法, 自动写 picks_tracking.json + DB)
    add_picks([entry], 'MANUAL', 1 - FIXED_STOP_PCT, 1 + TAKE_PROFIT_PCT,
              date=today, status='HOLDING')
    logger.info(f"Added manual position: {code} {name} @ {price}, "
                f"SL={sl_price}, TP={tp_price}{(' [VP·压力位卖出]' if vp_mode else '')}")

    # 6. 登记为真实持仓后, 清除 PLAN 计划池记录(避免重复提醒)
    if vp_mode:
        _remove_plan_record(code)

    # 7. Bark 成功通知
    if vp_mode:
        body = (
            f"{name}({code}) 已加入监控(价量计划·压力位卖出)\n"
            f"买入价: {price}\n"
            f"买点(支撑): {support}  卖点(压力): {pressure}\n"
            f"止损价: {sl_price}  止盈价(压力位): {tp_price}\n"
            f"日期: {today}\n"
            f"状态: HOLDING — 触及压力位将推送卖出提醒")
    else:
        body = (
            f"{name}({code}) 已加入监控\n"
            f"买入价: {price}\n"
            f"止损价: {sl_price} (-{FIXED_STOP_PCT*100:.0f}%)\n"
            f"止盈价: {tp_price} (+{TAKE_PROFIT_PCT*100:.0f}%)\n"
            f"日期: {today}\n"
            f"状态: HOLDING (已持仓，直接开始监控)")
    bark_notify("📌 手动持仓已录入", body)
    logger.info("Done.")


if __name__ == '__main__':
    main()
