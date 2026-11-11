# -*- coding: utf-8 -*-
"""Bark 推送通知模块"""
import requests, logging
logger = logging.getLogger('momentum')
BARK_URL = "https://api.day.app/push"

# Bark 服务端按 **UTF-8 字节** 限制 payload(超限返回 500 PayloadTooLarge)。
# 旧实现按字符 body[:3800] 截断, 3800 个中文≈11KB 仍会超限, 故改为按字节切分并分条推送。
MAX_BODY_BYTES = 3500


def _split_utf8(text: str, limit: int = MAX_BODY_BYTES):
    """按 UTF-8 字节数切分文本, 尽量以行为边界切, 保证每段 <= limit 字节。"""
    if len(text.encode('utf-8')) <= limit:
        return [text]
    chunks, cur, cur_b = [], [], 0
    for line in text.splitlines(keepends=True):
        lb = len(line.encode('utf-8'))
        if lb > limit:                      # 单行就超限 -> 逐字符切
            for ch in line:
                cb = len(ch.encode('utf-8'))
                if cur_b + cb > limit:
                    chunks.append(''.join(cur)); cur, cur_b = [], 0
                cur.append(ch); cur_b += cb
            continue
        if cur_b + lb > limit:
            chunks.append(''.join(cur)); cur, cur_b = [], 0
        cur.append(line); cur_b += lb
    if cur:
        chunks.append(''.join(cur))
    return chunks or ['']


def send_bark(title: str, content: str, device_key: str = None, icon: str = None):
    """发送 Bark 推送，统一用 POST 避免 GET URL 超长 431；超长内容自动分条。"""
    from .. import config as cfg

    key = device_key or getattr(cfg, 'BARK_DEVICE_KEY', '').strip()
    if not key:
        logger.warning("Bark device_key not configured")
        return

    # URL 格式 → 提取末尾 device key
    if key.startswith('http'):
        parts = key.rstrip('/').split('/')
        key = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else key)

    chunks = _split_utf8(content or '')
    total = len(chunks)
    for i, chunk in enumerate(chunks, 1):
        t = title if total == 1 else f"{title} ({i}/{total})"
        payload = {"device_key": key, "title": t, "body": chunk, "group": "Momentum"}
        if icon:
            payload["icon"] = icon
        try:
            r = requests.post(BARK_URL, json=payload, timeout=10)
            if r.status_code == 200:
                logger.info("Bark sent%s", "" if total == 1 else f" [{i}/{total}]")
            else:
                logger.error(f"Bark failed: {r.text}")
        except Exception as e:
            logger.error(f"Bark error: {e}")


def send_msg(title: str, content: str):
    """统一入口，替代 send_feishu_msg"""
    send_bark(title, content)


def send_card(title: str, fields: list):
    """统一入口，替代 send_feishu_card"""
    text = "\n".join(f"▪ {f.get('title','')}: {f.get('value','')}" for f in fields)
    send_bark(title, text)
