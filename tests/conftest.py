# -*- coding: utf-8 -*-
"""
pytest配置文件

定义全局fixtures和测试配置
"""

import pytest
import sys
import os

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def pytest_configure(config):
    """pytest配置钩子"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )


@pytest.fixture(scope='session')
def test_data_dir():
    """测试数据目录"""
    return os.path.join(os.path.dirname(__file__), 'test_data')


@pytest.fixture(scope='session')
def sample_stock_codes():
    """示例股票代码"""
    return ['000001', '000002', '000063', '000333', '000651']


@pytest.fixture
def mock_config():
    """模拟配置"""
    return {
        'initial_capital': 1000000,
        'max_positions': 5,
        'hold_period': 5,
        'stop_loss': 0.05,
        'take_profit': 0.10,
        'slippage': 0.008,
    }
