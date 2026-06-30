# -*- coding: utf-8 -*-
"""Bark 推送通知模块"""
import requests, logging
logger = logging.getLogger('momentum')
BARK_URL = "https://api.day.app/push"


def send_bark(title: str, content: str, device_key: str = None, icon: str = None):
    """发送 Bark 推送，自动适配 URL key / 纯 key 格式"""
    from .. import config as cfg
    from urllib.parse import quote

    key = device_key or getattr(cfg, 'BARK_DEVICE_KEY', '').strip()
    if not key:
        logger.warning("Bark device_key not configured")
        return

    # 截断过长内容 (手机屏幕友好)
    body = content[:3800]

    if key.startswith('http'):
        server = key.rstrip('/')
        # GET 格式: server/title/body?params
        params = []
        if icon: params.append(f"icon={quote(icon)}")
        qs = "&".join(params)
        url = f"{server}/{quote(title)}/{quote(body)}"
        if qs: url += f"?{qs}"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200: logger.info("Bark sent")
            else: logger.error(f"Bark failed: {r.text}")
        except Exception as e:
            logger.error(f"Bark error: {e}")
    else:
        payload = {"device_key": key, "title": title, "body": body, "group": "Momentum"}
        if icon: payload["icon"] = icon
        try:
            r = requests.post(BARK_URL, json=payload, timeout=10)
            if r.status_code == 200: logger.info("Bark sent")
            else: logger.error(f"Bark failed: {r.text}")
        except Exception as e:
            logger.error(f"Bark error: {e}")


def send_msg(title: str, content: str):
    """统一入口，替代 send_feishu_msg"""
    send_bark(title, content)


def send_card(title: str, fields: list):
    """统一入口，替代 send_feishu_card"""
    text = "\n".join(f"▪ {f.get('title','')}: {f.get('value','')}" for f in fields)
    send_bark(title, text)
