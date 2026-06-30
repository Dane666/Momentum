# -*- coding: utf-8 -*-
"""Bark 推送通知模块"""
import requests, logging
logger = logging.getLogger('momentum')
BARK_URL = "https://api.day.app/push"


def send_bark(title: str, content: str, device_key: str = None):
    from .. import config as cfg
    from urllib.parse import quote
    key = device_key or getattr(cfg, 'BARK_DEVICE_KEY', '').strip()
    if not key:
        logger.warning("Bark device_key not configured")
        return
    # 支持两种格式: 纯 key 或完整 URL (https://api.day.app/xxxx)
    if key.startswith('http'):
        server = key.rstrip('/')
        try:
            r = requests.get(f"{server}/{quote(title)}/{quote(content[:4000])}?group=Momentum", timeout=10)
            if r.status_code == 200: logger.info("Bark sent")
            else: logger.error(f"Bark failed: {r.text}")
        except Exception as e:
            logger.error(f"Bark error: {e}")
    else:
        body = {"device_key": key, "title": title, "body": content[:4000], "group": "Momentum"}
        try:
            r = requests.post(BARK_URL, json=body, timeout=10)
            if r.status_code == 200: logger.info("Bark sent")
            else: logger.error(f"Bark failed: {r.text}")
        except Exception as e:
            logger.error(f"Bark error: {e}")


def send_msg(title: str, content: str):
    """统一入口，替代 send_feishu_msg"""
    send_bark(title, content)


def send_card(title: str, fields: list):
    """统一入口，替代 send_feishu_card"""
    text = "\n".join(f"{f.get('title','')}: {f.get('value','')}" for f in fields)
    send_bark(title, text)
