# -*- coding: utf-8 -*-
"""
代理防护模块

全局禁用代理，确保数据爬取接口正常运行
此模块应在所有其他导入之前导入
"""

import os
import logging

logger = logging.getLogger('momentum')

# 需要清除的代理环境变量
PROXY_KEYS = [
    'http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY',
    'all_proxy', 'ALL_PROXY', 'ftp_proxy', 'FTP_PROXY',
    'no_proxy', 'NO_PROXY'
]


def disable_proxy():
    """
    禁用所有代理设置
    
    应在程序入口处调用，确保在任何网络请求之前执行
    """
    removed = []
    for key in PROXY_KEYS:
        if key in os.environ:
            del os.environ[key]
            removed.append(key)
    
    # 设置 no_proxy 为通配符，确保所有请求直连
    os.environ['no_proxy'] = '*'
    os.environ['NO_PROXY'] = '*'
    
    if removed:
        logger.debug(f"[ProxyGuard] 已禁用代理: {removed}")


def patch_requests_session():
    """
    修补 requests.Session 以禁用代理
    """
    try:
        import requests
        
        _original_init = requests.Session.__init__
        
        def _patched_init(self, *args, **kwargs):
            _original_init(self, *args, **kwargs)
            self.trust_env = False  # 不使用环境变量中的代理
            self.proxies = {}       # 清空代理配置
        
        requests.Session.__init__ = _patched_init
        logger.debug("[ProxyGuard] 已修补 requests.Session")
    except Exception as e:
        logger.debug(f"[ProxyGuard] 修补 requests.Session 失败: {e}")


# 模块加载时自动执行
disable_proxy()
patch_requests_session()
