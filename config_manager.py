# -*- coding: utf-8 -*-
"""
参数版本管理系统

支持：
1. 参数版本控制
2. 配置回滚
3. A/B测试对比
4. 参数持久化
"""

import json
import os
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, Optional, List, Any
import logging

logger = logging.getLogger('momentum.config')


@dataclass
class StrategyVersion:
    """策略参数版本"""
    version: str                     # 版本号 (如 "v1.2.0")
    created_at: str                  # 创建时间
    description: str                 # 版本描述
    status: str                      # 状态: development/testing/production
    
    # 回测参数
    backtest_days: int = 120
    hold_period: int = 3
    pool_size: int = 150
    slippage: float = 0.008
    initial_capital: float = 100000.0
    
    # 选股参数
    min_change_pct: float = 4.0
    max_change_pct: float = 9.2
    min_vol_ratio: float = 1.2
    min_amount: int = 200000000
    min_sharpe: float = 1.0
    max_sector_picks: int = 1
    max_total_picks: int = 1
    
    # 风控参数
    rsi_danger_zone: float = 80.0
    fixed_stop_pct: float = 0.05
    take_profit_pct: float = 0.10
    bias_profit_limit: float = 0.20
    atr_stop_factor: float = 1.2
    
    # 因子权重
    weight_mom_5: float = 0.30
    weight_mom_20: float = 0.10
    weight_sharpe: float = 0.25
    weight_chip_rate: float = -0.15
    weight_big_order: float = 0.20
    
    # 性能指标 (回测后填充)
    sharpe_ratio: Optional[float] = None
    total_return: Optional[float] = None
    max_drawdown: Optional[float] = None
    win_rate: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StrategyVersion':
        """从字典创建"""
        return cls(**data)
    
    def get_factor_weights(self) -> Dict[str, float]:
        """获取因子权重字典"""
        return {
            'mom_5': self.weight_mom_5,
            'mom_20': self.weight_mom_20,
            'sharpe': self.weight_sharpe,
            'chip_rate': self.weight_chip_rate,
            'big_order': self.weight_big_order,
        }


class ConfigVersionManager:
    """
    配置版本管理器
    
    使用示例:
    ```python
    manager = ConfigVersionManager()
    
    # 创建新版本
    v1 = manager.create_version("v1.0.0", "初始版本", hold_period=3)
    
    # 切换到生产环境
    manager.promote_to_production("v1.0.0")
    
    # 获取当前生产配置
    config = manager.get_production_config()
    
    # 对比两个版本
    diff = manager.compare_versions("v1.0.0", "v1.1.0")
    ```
    """
    
    DEFAULT_CONFIG_PATH = "config_versions.json"
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化版本管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self.versions: Dict[str, StrategyVersion] = {}
        self.production_version: Optional[str] = None
        self._load()
    
    def _load(self):
        """从文件加载配置"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for version_id, version_data in data.get('versions', {}).items():
                    self.versions[version_id] = StrategyVersion.from_dict(version_data)
                
                self.production_version = data.get('production_version')
                logger.info(f"[ConfigManager] 加载 {len(self.versions)} 个版本配置")
            except Exception as e:
                logger.warning(f"[ConfigManager] 加载配置失败: {e}")
    
    def _save(self):
        """保存配置到文件"""
        try:
            data = {
                'versions': {k: v.to_dict() for k, v in self.versions.items()},
                'production_version': self.production_version,
                'updated_at': datetime.now().isoformat(),
            }
            
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"[ConfigManager] 配置已保存")
        except Exception as e:
            logger.error(f"[ConfigManager] 保存配置失败: {e}")
    
    def create_version(
        self,
        version: str,
        description: str,
        base_version: Optional[str] = None,
        **overrides
    ) -> StrategyVersion:
        """
        创建新版本
        
        Args:
            version: 版本号
            description: 版本描述
            base_version: 基于哪个版本创建
            **overrides: 覆盖的参数
            
        Returns:
            新创建的版本
        """
        if base_version and base_version in self.versions:
            base = self.versions[base_version].to_dict()
        else:
            base = {}
        
        # 合并参数
        params = {
            **base,
            'version': version,
            'created_at': datetime.now().isoformat(),
            'description': description,
            'status': 'development',
            **overrides
        }
        
        new_version = StrategyVersion(**params)
        self.versions[version] = new_version
        self._save()
        
        logger.info(f"[ConfigManager] 创建版本 {version}: {description}")
        return new_version
    
    def get_version(self, version: str) -> Optional[StrategyVersion]:
        """获取指定版本"""
        return self.versions.get(version)
    
    def get_production_config(self) -> Optional[StrategyVersion]:
        """获取当前生产环境配置"""
        if self.production_version:
            return self.versions.get(self.production_version)
        return None
    
    def promote_to_production(self, version: str) -> bool:
        """
        将版本提升为生产环境
        
        Args:
            version: 版本号
            
        Returns:
            是否成功
        """
        if version not in self.versions:
            logger.error(f"[ConfigManager] 版本不存在: {version}")
            return False
        
        # 更新状态
        old_prod = self.production_version
        if old_prod and old_prod in self.versions:
            self.versions[old_prod].status = 'archived'
        
        self.versions[version].status = 'production'
        self.production_version = version
        self._save()
        
        logger.info(f"[ConfigManager] 版本 {version} 已提升为生产环境")
        return True
    
    def compare_versions(
        self, 
        version_a: str, 
        version_b: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        对比两个版本的差异
        
        Returns:
            差异字典 {参数名: {old: ..., new: ...}}
        """
        if version_a not in self.versions or version_b not in self.versions:
            return {}
        
        a = self.versions[version_a].to_dict()
        b = self.versions[version_b].to_dict()
        
        diff = {}
        for key in set(a.keys()) | set(b.keys()):
            if a.get(key) != b.get(key):
                diff[key] = {
                    'old': a.get(key),
                    'new': b.get(key),
                }
        
        return diff
    
    def list_versions(self) -> List[Dict]:
        """列出所有版本"""
        result = []
        for version_id, version in sorted(self.versions.items()):
            result.append({
                'version': version_id,
                'status': version.status,
                'created_at': version.created_at,
                'description': version.description,
                'sharpe': version.sharpe_ratio,
                'is_production': version_id == self.production_version,
            })
        return result
    
    def update_metrics(
        self,
        version: str,
        sharpe: float,
        total_return: float,
        max_drawdown: float,
        win_rate: float
    ):
        """
        更新版本的性能指标
        
        Args:
            version: 版本号
            sharpe: 夏普比率
            total_return: 总收益率
            max_drawdown: 最大回撤
            win_rate: 胜率
        """
        if version not in self.versions:
            return
        
        v = self.versions[version]
        v.sharpe_ratio = sharpe
        v.total_return = total_return
        v.max_drawdown = max_drawdown
        v.win_rate = win_rate
        self._save()
    
    def rollback_to(self, version: str) -> bool:
        """
        回滚到指定版本
        
        Args:
            version: 目标版本号
            
        Returns:
            是否成功
        """
        return self.promote_to_production(version)


# ==================== 全局实例 ====================

_global_manager: Optional[ConfigVersionManager] = None


def get_config_manager() -> ConfigVersionManager:
    """获取全局配置管理器"""
    global _global_manager
    if _global_manager is None:
        _global_manager = ConfigVersionManager()
    return _global_manager


def get_current_config() -> StrategyVersion:
    """
    获取当前配置
    
    优先返回生产环境配置，否则返回默认配置
    """
    manager = get_config_manager()
    prod = manager.get_production_config()
    
    if prod:
        return prod
    
    # 返回默认配置
    return StrategyVersion(
        version="default",
        created_at=datetime.now().isoformat(),
        description="默认配置",
        status="default",
    )


def apply_config_to_module(config: StrategyVersion):
    """
    将配置应用到全局模块
    
    Args:
        config: 策略版本配置
    """
    try:
        from .. import config as cfg
        
        # 更新回测参数
        cfg.BACKTEST_DAYS_DEFAULT = config.backtest_days
        cfg.HOLD_PERIOD_DEFAULT = config.hold_period
        cfg.POOL_SIZE = config.pool_size
        cfg.SLIPPAGE = config.slippage
        cfg.INITIAL_CAPITAL = config.initial_capital
        
        # 更新选股参数
        cfg.MIN_CHANGE_PCT = config.min_change_pct
        cfg.MAX_CHANGE_PCT = config.max_change_pct
        cfg.MIN_VOL_RATIO = config.min_vol_ratio
        cfg.MIN_AMOUNT = config.min_amount
        cfg.MIN_SHARPE = config.min_sharpe
        cfg.MAX_SECTOR_PICKS = config.max_sector_picks
        cfg.MAX_TOTAL_PICKS = config.max_total_picks
        
        # 更新风控参数
        cfg.RSI_DANGER_ZONE = config.rsi_danger_zone
        cfg.FIXED_STOP_PCT = config.fixed_stop_pct
        cfg.TAKE_PROFIT_PCT = config.take_profit_pct
        cfg.BIAS_PROFIT_LIMIT = config.bias_profit_limit
        cfg.ATR_STOP_FACTOR = config.atr_stop_factor
        
        logger.info(f"[ConfigManager] 已应用配置 {config.version}")
    except ImportError:
        logger.warning("[ConfigManager] 无法导入config模块")
