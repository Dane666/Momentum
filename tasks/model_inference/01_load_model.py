# -*- coding: utf-8 -*-
"""
01_load_model.py —— 推理端加载模型
==================================
从模型文件(默认 tasks/model_training/models/model_v1.pkl, 或由 --model 指定 /
环境变量 LGBM_MODEL_PATH 指定, CI 上从 release 下载后传入) 加载模型与元数据。

供 02_generate_scores / 03_select_topk 复用。
"""
import sys
import json
import pickle
from pathlib import Path


def resolve_model_path(override=None):
    if override:
        return Path(override)
    env = __import__('os').environ.get('LGBM_MODEL_PATH')
    if env:
        return Path(env)
    return Path('tasks/model_training/models/model_v1.pkl')


def load_model(model_path=None):
    mp = resolve_model_path(model_path)
    if not mp.exists():
        raise FileNotFoundError(f'模型文件不存在: {mp}')
    with open(mp, 'rb') as f:
        bundle = pickle.load(f)
    meta_path = mp.parent / 'model_meta.json'
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    return bundle['model'], bundle.get('features', []), bundle.get('target', 'fwd20'), meta


if __name__ == '__main__':
    m, feats, tgt, meta = load_model()
    print('模型加载成功:', type(m).__name__)
    print('特征数:', len(feats))
    print('元数据:', meta.get('version'), '| 过拟合比', meta.get('metrics', {}).get('overfit_ratio'))
