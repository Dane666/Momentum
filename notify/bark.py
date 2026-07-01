# -*- coding: utf-8 -*-
"""Bark 推送通知模块"""
import requests, logging
logger = logging.getLogger('momentum')
BARK_URL = "https://api.day.app/push"


def send_bark(title: str, content: str, device_key: str = None, icon: str = None):
    """发送 Bark 推送，统一用 POST 避免 GET URL 超长 431"""
    from .. import config as cfg

    key = device_key or getattr(cfg, 'BARK_DEVICE_KEY', '').strip()
    if not key:
        logger.warning("Bark device_key not configured")
        return

    body = content[:3800]

    # URL 格式 → 提取末尾 device key
    if key.startswith('http'):
        parts = key.rstrip('/').split('/')
        key = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else key)

    payload = {"device_key": key, "title": title, "body": body, "group": "Momentum"}
    if icon:
        payload["icon"] = icon
    try:
        r = requests.post(BARK_URL, json=payload, timeout=10)
        if r.status_code == 200:
            logger.info("Bark sent")
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
