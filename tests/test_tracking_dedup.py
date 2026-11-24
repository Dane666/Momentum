# -*- coding: utf-8 -*-
"""tracking_utils.add_picks 同日同票(跨策略)去重测试。

规则: 同一交易日同一 code 若已存在(无论哪个策略 type), 以第一次入库记录为准,
后续策略重复选到该票则跳过, 不覆盖首次记录。
"""
import json
import os
import tempfile

from tools.tracking_utils import add_picks


def _read(path):
    return json.loads(open(path, encoding='utf-8').read())


def test_same_day_same_code_cross_strategy_dedup():
    """跨策略同日同票: 第一次入库为准, 后续跳过。"""
    tmp = tempfile.mktemp(suffix='.json')
    db = tempfile.mktemp(suffix='.db')
    try:
        # 第一次: LOW_QUALITY 选出 000430
        n1 = add_picks([{'code': '000430', 'name': '张家界', 'price': 5.29}],
                       'LOW_QUALITY', date='2026-11-23',
                       track_file=tmp, db_path=db)
        assert n1 == 1

        # 第二次: DEFENSIVE 也选到 000430(跨策略同日同票) -> 跳过
        n2 = add_picks([{'code': '000430', 'name': '张家界', 'price': 5.35},
                        {'code': '601398', 'name': '工商银行', 'price': 9.40}],
                       'DEFENSIVE', date='2026-11-23',
                       track_file=tmp, db_path=db)
        assert n2 == 1  # 只新增 601398

        data = _read(tmp)
        assert len(data) == 2
        zj = [p for p in data if p['code'] == '000430']
        assert len(zj) == 1
        # 保留第一次入库的 LOW_QUALITY + 原价 5.29, 不被 DEFENSIVE 覆盖
        assert zj[0]['type'] == 'LOW_QUALITY'
        assert zj[0]['price'] == 5.29
    finally:
        os.remove(tmp)
        if os.path.exists(db):
            os.remove(db)


def test_same_day_same_code_same_strategy_dedup():
    """同策略同日同票也跳过(幂等)。"""
    tmp = tempfile.mktemp(suffix='.json')
    db = tempfile.mktemp(suffix='.db')
    try:
        add_picks([{'code': '600000', 'name': '浦发银行', 'price': 10.0}],
                  'LOW_QUALITY', date='2026-11-23', track_file=tmp, db_path=db)
        n2 = add_picks([{'code': '600000', 'name': '浦发银行', 'price': 10.5}],
                       'LOW_QUALITY', date='2026-11-23', track_file=tmp, db_path=db)
        assert n2 == 0
        assert len(_read(tmp)) == 1
    finally:
        os.remove(tmp)
        if os.path.exists(db):
            os.remove(db)


def test_different_day_same_code_not_dedup():
    """跨日同票不去重(不同交易日的信号各自独立监控)。"""
    tmp = tempfile.mktemp(suffix='.json')
    db = tempfile.mktemp(suffix='.db')
    try:
        add_picks([{'code': '600000', 'name': '浦发银行', 'price': 10.0}],
                  'LOW_QUALITY', date='2026-11-20', track_file=tmp, db_path=db)
        n2 = add_picks([{'code': '600000', 'name': '浦发银行', 'price': 10.5}],
                       'LOW_QUALITY', date='2026-11-21', track_file=tmp, db_path=db)
        assert n2 == 1
        assert len(_read(tmp)) == 2
    finally:
        os.remove(tmp)
        if os.path.exists(db):
            os.remove(db)


def test_different_code_same_day_not_dedup():
    """同日不同票正常入库。"""
    tmp = tempfile.mktemp(suffix='.json')
    db = tempfile.mktemp(suffix='.db')
    try:
        n = add_picks([{'code': '600000', 'name': 'A', 'price': 10.0},
                       {'code': '600001', 'name': 'B', 'price': 20.0}],
                      'LOW_QUALITY', date='2026-11-23',
                      track_file=tmp, db_path=db)
        assert n == 2
        assert len(_read(tmp)) == 2
    finally:
        os.remove(tmp)
        if os.path.exists(db):
            os.remove(db)
