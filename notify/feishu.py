# -*- coding: utf-8 -*-
"""
飞书通知模块
"""

import requests
import json
import logging

logger = logging.getLogger('momentum')


def send_feishu_msg(title: str, content: str, webhook_url: str = None, enabled: bool = True):
    """
    发送飞书文本消息

    Args:
        title: 消息标题
        content: 消息内容
        webhook_url: Webhook URL (可选，默认从环境变量读取)
        enabled: 是否启用通知
    """
    if not enabled:
        return

    # 延迟导入避免循环依赖
    from .. import config as cfg

    url = (webhook_url or cfg.FEISHU_WEBHOOK_URL or "").strip()
    notification_enabled = cfg.ENABLE_FEISHU_NOTIFICATION if enabled is True else enabled
    if not notification_enabled:
        logger.info("飞书通知已禁用，跳过发送")
        return
    if not url:
        logger.warning("飞书通知已开启，但未配置 Webhook URL")
        return

    headers = {"Content-Type": "application/json"}
    data = {
        "msg_type": "text",
        "content": {"text": f"{title}\n\n{content}"}
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        if response.status_code != 200:
            logger.error(f"飞书消息发送失败: {response.text}")
        else:
            logger.info("飞书消息发送成功")
    except Exception as e:
        logger.error(f"飞书消息发送异常: {e}")


def send_feishu_card(title: str, fields: list, webhook_url: str = None, enabled: bool = True):
    """
    发送飞书卡片消息

    Args:
        title: 卡片标题
        fields: 字段列表 [{"title": "字段名", "value": "字段值"}, ...]
        webhook_url: Webhook URL
        enabled: 是否启用通知
    """
    if not enabled:
        return

    from .. import config as cfg

    url = (webhook_url or cfg.FEISHU_WEBHOOK_URL or "").strip()
    notification_enabled = cfg.ENABLE_FEISHU_NOTIFICATION if enabled is True else enabled
    if not notification_enabled:
        logger.info("飞书通知已禁用，跳过发送")
        return
    if not url:
        logger.warning("飞书通知已开启，但未配置 Webhook URL")
        return

    # 构建卡片内容
    elements = []
    for field in fields:
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**{field['title']}**: {field['value']}"
            }
        })

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue"
            },
            "elements": elements
        }
    }

    try:
        response = requests.post(url, headers={"Content-Type": "application/json"},
                                data=json.dumps(card), timeout=5)
        if response.status_code != 200:
            logger.error(f"飞书卡片发送失败: {response.text}")
        else:
            logger.info("飞书卡片发送成功")
    except Exception as e:
        logger.error(f"飞书卡片发送异常: {e}")
