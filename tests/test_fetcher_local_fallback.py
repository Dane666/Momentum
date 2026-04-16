# -*- coding: utf-8 -*-
"""
fetcher 本地兜底测试
"""

import pandas as pd
import importlib


def test_fetch_all_stock_codes_local_uses_adata_cache_when_default_file_missing(monkeypatch, tmp_path):
    from ..data import fetcher

    csv_path = tmp_path / 'code.csv'
    pd.DataFrame([
        {'stock_code': '1', 'short_name': '平安银行'},
        {'stock_code': '600000', 'short_name': '浦发银行'},
        {'stock_code': '830001', 'short_name': '北交所样本'},
        {'stock_code': '3', 'short_name': 'PT金田A'},
    ]).to_csv(csv_path, index=False)

    monkeypatch.delenv('MOMENTUM_CODE_LIST_FILE', raising=False)
    monkeypatch.delenv('MOMENTUM_CODE_LIMIT', raising=False)

    def fake_exists(path):
        return str(path) == str(csv_path)

    monkeypatch.setattr(fetcher.os.path, 'exists', fake_exists)
    cache_module = importlib.import_module('adata.stock.cache')
    monkeypatch.setattr(cache_module, 'get_code_csv_path', lambda: str(csv_path))

    codes = fetcher.fetch_all_stock_codes_local()

    assert codes == ['000001', '600000']
