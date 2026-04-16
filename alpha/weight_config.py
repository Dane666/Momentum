# -*- coding: utf-8 -*-
"""
Alpha权重优化模块

提供可配置的Alpha因子权重系统，支持：
1. 多组预设权重配置
2. 基于历史表现的动态调权
3. 参数网格搜索
"""

from dataclasses import dataclass
from typing import Dict, Optional, List
import pandas as pd
import numpy as np


@dataclass
class AlphaWeightConfig:
    """Alpha因子权重配置"""
    name: str                    # 配置名称
    mom_5: float = 0.30          # 5日动量
    mom_20: float = 0.10         # 20日动量
    sharpe: float = 0.25         # 夏普比率
    chip_rate: float = -0.15     # 筹码集中度 (负权重)
    big_order: float = 0.20      # 主力大单
    nlp_base: float = 0.30       # NLP基础权重
    
    def to_dict(self) -> Dict[str, float]:
        """转换为权重字典"""
        return {
            'mom_5': self.mom_5,
            'mom_20': self.mom_20,
            'sharpe': self.sharpe,
            'chip_rate': self.chip_rate,
            'big_order': self.big_order,
        }
    
    def validate(self) -> bool:
        """验证权重总和是否接近1"""
        # 负权重取绝对值
        total = abs(self.mom_5) + abs(self.mom_20) + abs(self.sharpe) + \
                abs(self.chip_rate) + abs(self.big_order)
        return 0.8 <= total <= 1.2  # 允许一定误差
    
    def __str__(self):
        return (f"AlphaWeights({self.name}): "
                f"mom5={self.mom_5:.2f}, mom20={self.mom_20:.2f}, "
                f"sharpe={self.sharpe:.2f}, chip={self.chip_rate:.2f}, "
                f"big_order={self.big_order:.2f}")


# ==================== 预设权重配置 ====================

# 默认配置 (当前使用)
DEFAULT_WEIGHTS = AlphaWeightConfig(
    name="default",
    mom_5=0.30,
    mom_20=0.10,
    sharpe=0.25,
    chip_rate=-0.15,
    big_order=0.20,
)

# 动量优先配置 (短期反转)
MOMENTUM_FIRST_WEIGHTS = AlphaWeightConfig(
    name="momentum_first",
    mom_5=0.40,        # ↑ 强化短期动量
    mom_20=0.15,       # ↑ 强化中期趋势
    sharpe=0.20,       # ↓ 降低夏普权重
    chip_rate=-0.05,   # ↓ 弱化筹码因子
    big_order=0.20,    # 保持
)

# 质量优先配置 (稳健型)
QUALITY_FIRST_WEIGHTS = AlphaWeightConfig(
    name="quality_first",
    mom_5=0.20,        # ↓ 降低短期动量 
    mom_20=0.15,       # ↑ 强化趋势
    sharpe=0.35,       # ↑ 强化风险调整收益
    chip_rate=-0.10,   # 保持
    big_order=0.20,    # 保持
)

# 资金流优先配置 (跟随主力)
FLOW_FIRST_WEIGHTS = AlphaWeightConfig(
    name="flow_first",
    mom_5=0.25,        # 保持
    mom_20=0.10,       # 保持
    sharpe=0.20,       # ↓ 降低
    chip_rate=-0.15,   # 保持
    big_order=0.30,    # ↑ 强化大单因子
)

# 均衡配置 (等权)
BALANCED_WEIGHTS = AlphaWeightConfig(
    name="balanced",
    mom_5=0.20,
    mom_20=0.20,
    sharpe=0.20,
    chip_rate=-0.20,
    big_order=0.20,
)

# 所有预设配置
PRESET_CONFIGS: Dict[str, AlphaWeightConfig] = {
    "default": DEFAULT_WEIGHTS,
    "momentum_first": MOMENTUM_FIRST_WEIGHTS,
    "quality_first": QUALITY_FIRST_WEIGHTS,
    "flow_first": FLOW_FIRST_WEIGHTS,
    "balanced": BALANCED_WEIGHTS,
}


class AlphaWeightOptimizer:
    """
    Alpha权重优化器
    
    支持：
    1. 网格搜索最优权重
    2. 基于历史表现的自适应调权
    3. 敏感性分析
    """
    
    def __init__(self, base_config: Optional[AlphaWeightConfig] = None):
        """
        初始化优化器
        
        Args:
            base_config: 基础权重配置
        """
        self.base_config = base_config or DEFAULT_WEIGHTS
        self.history: List[Dict] = []  # 优化历史记录
    
    def generate_grid(
        self,
        param_ranges: Optional[Dict[str, List[float]]] = None
    ) -> List[AlphaWeightConfig]:
        """
        生成参数网格
        
        Args:
            param_ranges: 参数范围字典
            
        Returns:
            配置列表
        """
        if param_ranges is None:
            # 默认网格范围
            param_ranges = {
                'mom_5': [0.25, 0.30, 0.35, 0.40],
                'mom_20': [0.05, 0.10, 0.15],
                'sharpe': [0.20, 0.25, 0.30],
                'big_order': [0.15, 0.20, 0.25],
            }
        
        configs = []
        
        # 简化的网格搜索 (避免组合爆炸)
        for mom5 in param_ranges.get('mom_5', [0.30]):
            for mom20 in param_ranges.get('mom_20', [0.10]):
                for sharpe in param_ranges.get('sharpe', [0.25]):
                    for big_order in param_ranges.get('big_order', [0.20]):
                        # 自动计算chip_rate使总权重为1
                        chip_rate = -(1.0 - mom5 - mom20 - sharpe - big_order)
                        
                        config = AlphaWeightConfig(
                            name=f"grid_{mom5}_{mom20}_{sharpe}_{big_order}",
                            mom_5=mom5,
                            mom_20=mom20,
                            sharpe=sharpe,
                            chip_rate=chip_rate,
                            big_order=big_order,
                        )
                        configs.append(config)
        
        return configs
    
    def recommend_weights(
        self,
        market_condition: str = "normal"
    ) -> AlphaWeightConfig:
        """
        根据市场状态推荐权重配置
        
        Args:
            market_condition: 市场状态 (bullish/bearish/normal/volatile)
            
        Returns:
            推荐的权重配置
        """
        if market_condition == "bullish":
            # 牛市强化动量
            return MOMENTUM_FIRST_WEIGHTS
        elif market_condition == "bearish":
            # 熊市强化质量
            return QUALITY_FIRST_WEIGHTS
        elif market_condition == "volatile":
            # 震荡市强化大单跟随
            return FLOW_FIRST_WEIGHTS
        else:
            # 常规市场用均衡配置
            return DEFAULT_WEIGHTS
    
    def analyze_sensitivity(
        self,
        factor_name: str,
        values: List[float],
        evaluate_func
    ) -> pd.DataFrame:
        """
        单因子敏感性分析
        
        Args:
            factor_name: 因子名称 (如 'mom_5')
            values: 测试值列表
            evaluate_func: 评估函数，接收config返回性能指标
            
        Returns:
            敏感性分析结果DataFrame
        """
        results = []
        
        for value in values:
            config = AlphaWeightConfig(
                name=f"sensitivity_{factor_name}_{value}",
                **{**self.base_config.to_dict(), factor_name: value}
            )
            
            metrics = evaluate_func(config)
            results.append({
                'factor': factor_name,
                'value': value,
                'sharpe': metrics.get('sharpe', 0),
                'profit': metrics.get('profit_pct', 0),
                'win_rate': metrics.get('win_rate', 0),
            })
        
        return pd.DataFrame(results)


def get_weight_config(name: str = "default") -> AlphaWeightConfig:
    """
    获取预设权重配置
    
    Args:
        name: 配置名称
        
    Returns:
        AlphaWeightConfig 实例
    """
    return PRESET_CONFIGS.get(name, DEFAULT_WEIGHTS)


def apply_weights_to_alpha_model(config: AlphaWeightConfig) -> Dict[str, float]:
    """
    将权重配置应用到AlphaModel
    
    Args:
        config: 权重配置
        
    Returns:
        适用于AlphaModel的权重字典
    """
    return config.to_dict()
