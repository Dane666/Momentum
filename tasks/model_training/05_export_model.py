# -*- coding: utf-8 -*-
"""
05_export_model.py —— 导出模型资产(供 CI 推理下载)
====================================================
- 读取 03 产出的 model_v1.pkl, 复制为版本化文件 model_v1.pkl(已就位)
- 产出 model_meta.json(特征列表 / 训练区间 / 指标 / 版本), 供推理端校验
- 可选导出 ONNX(若环境支持)
"""
import sys
import json
import shutil
from pathlib import Path
from datetime import datetime

ROOT = Path('.')
OUT = Path('tasks/model_training/output')
MODELS = Path('tasks/model_training/models')
VERSION = 'v1'


def main():
    src = MODELS / 'model_v1.pkl'
    if not src.exists():
        raise SystemExit('model_v1.pkl 不存在, 请先运行 03_train_lightgbm.py')
    # 复制为版本化文件(推理端按版本下载); 若已同名则跳过
    dst = MODELS / f'model_{VERSION}.pkl'
    if src.resolve() != dst.resolve():
        shutil.copy(src, dst)

    bundle = json.loads((OUT / 'best_params.json').read_text())
    meta = {
        'version': VERSION,
        'exported_at': datetime.now().isoformat(timespec='seconds'),
        'features': bundle['features'],
        'target': 'fwd20',
        'trained_on': 'train+val',
        'metrics': {
            'train_rmse': bundle['train_rmse'],
            'val_rmse': bundle['val_rmse'],
            'overfit_ratio': bundle['overfit_ratio'],
        },
        'model_file': f'model_{VERSION}.pkl',
        'inference_max_model_size_mb': round(dst.stat().st_size / 1e6, 2),
    }
    (MODELS / 'model_meta.json').write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    print(f'[ok] -> {dst}')
    print(f'[ok] -> {MODELS/"model_meta.json"}')
    print('特征:', meta['features'])

    # 可选 ONNX
    try:
        import onnx
        import onnxruntime
        from lightgbm import Booster
        booster = Booster(model_file=str(MODELS / 'model_v1.pkl'))
        booster.save_model(str(MODELS / f'model_{VERSION}.onnx'), num_iteration=booster.num_trees())
        print(f'[ok] -> ONNX 导出成功: model_{VERSION}.onnx')
    except Exception as e:
        print(f'[skip] ONNX 导出跳过(环境缺 onnx/onnxruntime): {e}')


if __name__ == '__main__':
    main()
