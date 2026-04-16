# -*- coding: utf-8 -*-
"""
策略健康监控系统

实时监控策略运行状态，检测异常并发送告警。

监控指标:
- 实时胜率变化
- 回撤阈值告警
- 交易频率异常
- 数据源可用性
- 因子有效性衰减
"""

import os
import json
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, List, Callable, Any
from enum import Enum
import logging

logger = logging.getLogger('momentum.monitor')


class AlertLevel(Enum):
    """告警级别"""
    INFO = 'INFO'
    WARNING = 'WARNING'
    CRITICAL = 'CRITICAL'


@dataclass
class Alert:
    """告警信息"""
    level: AlertLevel
    metric: str
    message: str
    value: float
    threshold: float
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            'level': self.level.value,
            'metric': self.metric,
            'message': self.message,
            'value': self.value,
            'threshold': self.threshold,
            'timestamp': self.timestamp.isoformat(),
        }


@dataclass
class HealthStatus:
    """健康状态"""
    overall: str  # HEALTHY, WARNING, CRITICAL
    win_rate: float
    drawdown: float
    trade_count: int
    data_freshness: bool
    factor_decay: float
    alerts: List[Alert]
    checked_at: datetime = field(default_factory=datetime.now)


class HealthChecker:
    """
    策略健康检查器
    
    使用示例:
    ```python
    checker = HealthChecker(db_path='qlib_pro_v16.db')
    
    # 运行健康检查
    status = checker.check_all()
    
    print(f"整体状态: {status.overall}")
    print(f"当前胜率: {status.win_rate:.1%}")
    print(f"当前回撤: {status.drawdown:.1%}")
    
    # 查看告警
    for alert in status.alerts:
        print(f"[{alert.level.value}] {alert.message}")
    ```
    """
    
    # 默认阈值
    THRESHOLDS = {
        'win_rate_min': 0.40,           # 胜率下限
        'win_rate_warning': 0.45,       # 胜率警告线
        'drawdown_warning': 0.15,       # 回撤警告线
        'drawdown_critical': 0.25,      # 回撤危险线
        'trade_min_daily': 1,           # 每日最少交易
        'trade_max_daily': 20,          # 每日最多交易
        'data_stale_hours': 24,         # 数据过期时间
        'factor_decay_warning': 0.20,   # 因子衰减警告
        'consecutive_loss': 5,          # 连续亏损次数
        'recent_loss_rate': 0.60,       # 近期亏损率
    }
    
    def __init__(
        self,
        db_path: str = None,
        thresholds: Dict[str, float] = None,
    ):
        """
        初始化健康检查器
        
        Args:
            db_path: 数据库路径
            thresholds: 自定义阈值
        """
        self.db_path = db_path
        self.thresholds = {**self.THRESHOLDS, **(thresholds or {})}
        self.alerts: List[Alert] = []
        self.alert_callbacks: List[Callable[[Alert], None]] = []
    
    def add_alert_callback(self, callback: Callable[[Alert], None]):
        """添加告警回调"""
        self.alert_callbacks.append(callback)
    
    def _emit_alert(self, alert: Alert):
        """触发告警"""
        self.alerts.append(alert)
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")
    
    def check_all(self) -> HealthStatus:
        """
        运行全部健康检查
        
        Returns:
            HealthStatus 健康状态对象
        """
        self.alerts = []
        
        # 检查各项指标
        win_rate = self.check_win_rate()
        drawdown = self.check_drawdown()
        trade_count = self.check_trade_frequency()
        data_fresh = self.check_data_freshness()
        factor_decay = self.check_factor_decay()
        
        # 检查连续亏损
        self.check_consecutive_losses()
        
        # 检查近期表现
        self.check_recent_performance()
        
        # 确定整体状态
        if any(a.level == AlertLevel.CRITICAL for a in self.alerts):
            overall = 'CRITICAL'
        elif any(a.level == AlertLevel.WARNING for a in self.alerts):
            overall = 'WARNING'
        else:
            overall = 'HEALTHY'
        
        return HealthStatus(
            overall=overall,
            win_rate=win_rate,
            drawdown=drawdown,
            trade_count=trade_count,
            data_freshness=data_fresh,
            factor_decay=factor_decay,
            alerts=self.alerts.copy(),
        )
    
    def check_win_rate(self) -> float:
        """检查胜率"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 获取最近30笔交易
            df = pd.read_sql_query(
                """
                SELECT return_pct FROM backtest_trades 
                ORDER BY exit_date DESC 
                LIMIT 30
                """,
                conn
            )
            conn.close()
            
            if len(df) == 0:
                return 0.0
            
            win_rate = (df['return_pct'] > 0).mean()
            
            if win_rate < self.thresholds['win_rate_min']:
                self._emit_alert(Alert(
                    level=AlertLevel.CRITICAL,
                    metric='win_rate',
                    message=f'胜率严重下降: {win_rate:.1%} < {self.thresholds["win_rate_min"]:.1%}',
                    value=win_rate,
                    threshold=self.thresholds['win_rate_min'],
                ))
            elif win_rate < self.thresholds['win_rate_warning']:
                self._emit_alert(Alert(
                    level=AlertLevel.WARNING,
                    metric='win_rate',
                    message=f'胜率下降预警: {win_rate:.1%}',
                    value=win_rate,
                    threshold=self.thresholds['win_rate_warning'],
                ))
            
            return win_rate
            
        except Exception as e:
            logger.error(f"Win rate check error: {e}")
            return 0.0
    
    def check_drawdown(self) -> float:
        """检查回撤"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 获取最近回测的回撤
            df = pd.read_sql_query(
                """
                SELECT max_drawdown FROM backtest_sessions 
                ORDER BY end_time DESC 
                LIMIT 1
                """,
                conn
            )
            conn.close()
            
            if len(df) == 0:
                return 0.0
            
            drawdown = df['max_drawdown'].iloc[0] / 100 if df['max_drawdown'].iloc[0] > 1 else df['max_drawdown'].iloc[0]
            
            if drawdown > self.thresholds['drawdown_critical']:
                self._emit_alert(Alert(
                    level=AlertLevel.CRITICAL,
                    metric='drawdown',
                    message=f'回撤超限: {drawdown:.1%} > {self.thresholds["drawdown_critical"]:.1%}',
                    value=drawdown,
                    threshold=self.thresholds['drawdown_critical'],
                ))
            elif drawdown > self.thresholds['drawdown_warning']:
                self._emit_alert(Alert(
                    level=AlertLevel.WARNING,
                    metric='drawdown',
                    message=f'回撤预警: {drawdown:.1%}',
                    value=drawdown,
                    threshold=self.thresholds['drawdown_warning'],
                ))
            
            return drawdown
            
        except Exception as e:
            logger.error(f"Drawdown check error: {e}")
            return 0.0
    
    def check_trade_frequency(self) -> int:
        """检查交易频率"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 统计最近7天每日交易数
            df = pd.read_sql_query(
                """
                SELECT entry_date, COUNT(*) as cnt 
                FROM backtest_trades 
                WHERE entry_date >= date('now', '-7 days')
                GROUP BY entry_date
                """,
                conn
            )
            conn.close()
            
            if len(df) == 0:
                return 0
            
            avg_daily = df['cnt'].mean()
            total = df['cnt'].sum()
            
            if avg_daily > self.thresholds['trade_max_daily']:
                self._emit_alert(Alert(
                    level=AlertLevel.WARNING,
                    metric='trade_frequency',
                    message=f'交易频率过高: 日均 {avg_daily:.1f} 笔',
                    value=avg_daily,
                    threshold=self.thresholds['trade_max_daily'],
                ))
            
            return int(total)
            
        except Exception as e:
            logger.error(f"Trade frequency check error: {e}")
            return 0
    
    def check_data_freshness(self) -> bool:
        """检查数据新鲜度"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            df = pd.read_sql_query(
                """
                SELECT MAX(trade_date) as latest FROM kline_cache
                """,
                conn
            )
            conn.close()
            
            if len(df) == 0 or df['latest'].iloc[0] is None:
                self._emit_alert(Alert(
                    level=AlertLevel.WARNING,
                    metric='data_freshness',
                    message='无可用K线数据',
                    value=0,
                    threshold=0,
                ))
                return False
            
            latest = pd.to_datetime(df['latest'].iloc[0])
            age_hours = (datetime.now() - latest).total_seconds() / 3600
            
            if age_hours > self.thresholds['data_stale_hours']:
                self._emit_alert(Alert(
                    level=AlertLevel.WARNING,
                    metric='data_freshness',
                    message=f'数据过期: 最新数据 {age_hours:.0f} 小时前',
                    value=age_hours,
                    threshold=self.thresholds['data_stale_hours'],
                ))
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Data freshness check error: {e}")
            return False
    
    def check_factor_decay(self) -> float:
        """检查因子衰减"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 获取因子日志（如果有）
            try:
                df = pd.read_sql_query(
                    """
                    SELECT * FROM factor_logs 
                    ORDER BY log_time DESC 
                    LIMIT 100
                    """,
                    conn
                )
            except Exception:
                # 表不存在
                conn.close()
                return 0.0
            
            conn.close()
            
            if len(df) == 0:
                return 0.0
            
            # 简单计算：对比前后期因子IC
            # 这里用占位逻辑，实际需要根据factor_logs表结构实现
            decay = 0.0
            
            if decay > self.thresholds['factor_decay_warning']:
                self._emit_alert(Alert(
                    level=AlertLevel.WARNING,
                    metric='factor_decay',
                    message=f'因子衰减预警: {decay:.1%}',
                    value=decay,
                    threshold=self.thresholds['factor_decay_warning'],
                ))
            
            return decay
            
        except Exception as e:
            logger.error(f"Factor decay check error: {e}")
            return 0.0
    
    def check_consecutive_losses(self):
        """检查连续亏损"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            df = pd.read_sql_query(
                """
                SELECT return_pct FROM backtest_trades 
                ORDER BY exit_date DESC 
                LIMIT 20
                """,
                conn
            )
            conn.close()
            
            if len(df) == 0:
                return
            
            # 计算连续亏损次数
            consecutive = 0
            for ret in df['return_pct']:
                if ret < 0:
                    consecutive += 1
                else:
                    break
            
            if consecutive >= self.thresholds['consecutive_loss']:
                self._emit_alert(Alert(
                    level=AlertLevel.CRITICAL,
                    metric='consecutive_loss',
                    message=f'连续亏损 {consecutive} 笔!',
                    value=consecutive,
                    threshold=self.thresholds['consecutive_loss'],
                ))
            
        except Exception as e:
            logger.error(f"Consecutive loss check error: {e}")
    
    def check_recent_performance(self):
        """检查近期表现"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            df = pd.read_sql_query(
                """
                SELECT return_pct FROM backtest_trades 
                ORDER BY exit_date DESC 
                LIMIT 10
                """,
                conn
            )
            conn.close()
            
            if len(df) < 5:
                return
            
            loss_rate = (df['return_pct'] < 0).mean()
            
            if loss_rate >= self.thresholds['recent_loss_rate']:
                self._emit_alert(Alert(
                    level=AlertLevel.WARNING,
                    metric='recent_performance',
                    message=f'近10笔交易亏损率: {loss_rate:.0%}',
                    value=loss_rate,
                    threshold=self.thresholds['recent_loss_rate'],
                ))
            
        except Exception as e:
            logger.error(f"Recent performance check error: {e}")


class AlertNotifier:
    """
    告警通知器
    
    支持多种通知渠道：控制台、文件、企业微信等
    """
    
    def __init__(self, log_file: str = None):
        self.log_file = log_file
        self.history: List[Alert] = []
    
    def notify_console(self, alert: Alert):
        """控制台输出"""
        level_colors = {
            AlertLevel.INFO: '\033[94m',      # 蓝色
            AlertLevel.WARNING: '\033[93m',   # 黄色
            AlertLevel.CRITICAL: '\033[91m',  # 红色
        }
        reset = '\033[0m'
        color = level_colors.get(alert.level, '')
        
        print(f"{color}[{alert.level.value}] {alert.message}{reset}")
    
    def notify_file(self, alert: Alert):
        """写入日志文件"""
        if self.log_file is None:
            return
        
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(alert.to_dict(), ensure_ascii=False) + '\n')
        except Exception as e:
            logger.error(f"File notify error: {e}")
    
    def notify_feishu(self, alert: Alert, webhook_url: str = None):
        """飞书通知"""
        try:
            from ..notify.feishu import send_feishu_card
            
            # 根据告警级别设置标题
            level_icons = {
                AlertLevel.INFO: 'ℹ️',
                AlertLevel.WARNING: '⚠️',
                AlertLevel.CRITICAL: '🚨',
            }
            icon = level_icons.get(alert.level, '📢')
            title = f"{icon} 策略告警 [{alert.level.value}]"
            
            fields = [
                {'title': '告警信息', 'value': alert.message},
                {'title': '监控指标', 'value': alert.metric},
                {'title': '当前值', 'value': f'{alert.value:.4f}'},
                {'title': '阈值', 'value': f'{alert.threshold:.4f}'},
                {'title': '时间', 'value': alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')},
            ]
            
            send_feishu_card(title=title, fields=fields, webhook_url=webhook_url, enabled=True)
        except Exception as e:
            logger.error(f"Feishu notify error: {e}")
    
    def __call__(self, alert: Alert):
        """默认通知：控制台 + 文件"""
        self.history.append(alert)
        self.notify_console(alert)
        self.notify_file(alert)


# ========== 便捷函数 ==========

def quick_health_check(db_path: str) -> HealthStatus:
    """
    快速健康检查
    
    Args:
        db_path: 数据库路径
        
    Returns:
        健康状态
    """
    checker = HealthChecker(db_path=db_path)
    notifier = AlertNotifier()
    checker.add_alert_callback(notifier)
    
    return checker.check_all()


def setup_monitoring(
    db_path: str,
    log_file: str = None,
    feishu_webhook: str = None,
) -> HealthChecker:
    """
    设置监控系统
    
    Args:
        db_path: 数据库路径
        log_file: 日志文件路径
        feishu_webhook: 飞书webhook URL
        
    Returns:
        配置好的健康检查器
    """
    checker = HealthChecker(db_path=db_path)
    notifier = AlertNotifier(log_file=log_file)
    
    # 添加控制台和文件通知
    checker.add_alert_callback(notifier)
    
    # 添加飞书通知（仅CRITICAL级别）
    if feishu_webhook:
        def feishu_callback(alert: Alert):
            if alert.level == AlertLevel.CRITICAL:
                notifier.notify_feishu(alert, feishu_webhook)
        
        checker.add_alert_callback(feishu_callback)
    
    return checker
