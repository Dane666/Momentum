# -*- coding: utf-8 -*-
"""config_loader —— 读取 config/regime_config.yaml(项目根相对路径)。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path('.').resolve()


def load_regime_config(path: str = None) -> dict:
    """加载 regime_config.yaml。优先 path 参数, 否则项目根 config/regime_config.yaml。"""
    import yaml
    p = Path(path) if path else ROOT / 'config' / 'regime_config.yaml'
    if not p.exists():
        raise FileNotFoundError(f'未找到配置: {p}')
    return yaml.safe_load(p.read_text(encoding='utf-8'))
