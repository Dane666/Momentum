# -*- coding: utf-8 -*-
"""
03_train_lightgbm.py —— 模型训练 + 超参数调优(Optuna)
====================================================
- 用 selected_features.json 的特征
- Optuna 在 train 上训练、val 上早停/选参(目标最小化 val RMSE)
- 最终模型在 train+val 上重训, 保存 tasks/model_training/models/model_v1.pkl
- 同时保存 best_params.json 与训练集/验证集指标(过拟合检测用)
"""
import sys
import json
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
import optuna

ROOT = Path('.')
OUT = Path('tasks/model_training/output')
MODELS = Path('tasks/model_training/models')
MODELS.mkdir(parents=True, exist_ok=True)

TARGET = 'fwd20'
N_TRIALS = 50


def rmse(a, b):
    return float(np.sqrt(np.mean((np.asarray(a) - np.asarray(b)) ** 2)))


def load():
    df = pd.read_parquet(OUT / 'model_dataset.parquet')
    feats = json.loads((OUT / 'selected_features.json').read_text())['features']
    tr = df[df.split == 'train']
    va = df[df.split == 'val']
    return feats, tr, va


def objective(trial, feats, tr, va):
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 300, 1200, step=100),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.05, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 16, 64),
        'min_child_samples': trial.suggest_int('min_child_samples', 100, 500, step=50),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'reg_lambda': trial.suggest_float('reg_lambda', 0.1, 5.0, log=True),
        'random_state': 42, 'n_jobs': -1, 'verbosity': -1,
    }
    model = lgb.LGBMRegressor(**params)
    model.fit(tr[feats], tr[TARGET], eval_X=va[feats], eval_y=va[TARGET],
              eval_metric='rmse',
              callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)])
    pred = model.predict(va[feats])
    return rmse(va[TARGET], pred)


def main():
    print('[1/3] 加载数据 + 特征 ...')
    feats, tr, va = load()
    print(f'     特征={len(feats)} train={len(tr)} val={len(va)}')

    print(f'[2/3] Optuna 调参 ({N_TRIALS} trials) ...')
    study = optuna.create_study(direction='minimize',
                                sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(lambda t: objective(t, feats, tr, va), n_trials=N_TRIALS,
                   show_progress_bar=False)
    best = study.best_params
    best_rmse = study.best_value
    print(f'     最佳 val RMSE={best_rmse:.5f}')
    print(f'     最佳参数={best}')

    print('[3/3] 在 train+val 上重训最终模型 ...')
    trva = pd.concat([tr, va], ignore_index=True)
    final = lgb.LGBMRegressor(**best, random_state=42, n_jobs=-1, verbosity=-1)
    final.fit(trva[feats], trva[TARGET])

    tr_pred = final.predict(tr[feats]); va_pred = final.predict(va[feats])
    metrics = {
        'features': feats,
        'train_rmse': rmse(tr[TARGET], tr_pred),
        'val_rmse': rmse(va[TARGET], va_pred),
        'best_params': best,
        'optuna_best_val_rmse': best_rmse,
        'n_train': len(tr), 'n_val': len(va), 'n_trainval': len(trva),
    }
    # 过拟合检测: val/train RMSE 比
    metrics['overfit_ratio'] = metrics['val_rmse'] / metrics['train_rmse']
    import pickle
    with open(MODELS / 'model_v1.pkl', 'wb') as f:
        pickle.dump({'model': final, 'features': feats, 'target': TARGET,
                     'trained_on': 'train+val', 'metrics': metrics}, f)
    # 原生 Booster 文本格式(版本无关): 避免 sklearn 包装器 pickle 跨版本 get_params 报错.
    # CI 推理端 load_model 优先加载 model_v1.txt.
    try:
        final.booster_.save_model(str(MODELS / 'model_v1.txt'))
        print(f'[ok] -> {MODELS/"model_v1.txt"} (Booster 文本格式, 版本无关)')
    except Exception as e:
        print(f'[skip] Booster 文本导出失败: {e}')
    (OUT / 'best_params.json').write_text(json.dumps(metrics, indent=2))
    print(f'     train RMSE={metrics["train_rmse"]:.5f}  val RMSE={metrics["val_rmse"]:.5f}'
          f'  过拟合比={metrics["overfit_ratio"]:.3f}')
    print(f'[ok] -> {MODELS/"model_v1.pkl"}')
    print(f'[ok] -> {OUT/"best_params.json"}')


if __name__ == '__main__':
    main()
