# -*- coding: utf-8 -*-
"""
01_regime_detector.py —— 市场状态识别 (RegimeDetector)
========================================================
输入: 沪深300(000300)指数日线 OHLCV(close/open/high/low/volume/trade_date)
输出: 每个交易日的市场状态, 取值之一:
        'trend_up'   趋势上行  (价格站稳 MA 之上, 中期趋势向上, 波动正常)
        'trend_down' 趋势下行  (价格跌破 MA, 中期趋势向下)
        'range'      震荡      (无明显趋势, 波动正常)
        'high_vol'   高波动    (短期已实现波动率突破高位阈值, 优先于趋势判定)

判定逻辑(可解释、可配置, 阈值在 config/regime_config.yaml):
  1. 高波动优先: 若 20 日年化波动率 > high_vol_quantile 分位(或绝对阈值),
     直接判 'high_vol'(无论涨跌, 风险预算都要收缩)。
  2. 趋势方向: 用 MA20 / MA60 的关系 + 价格相对 MA60 的位置判定 trend_up / trend_down:
       - 价格 > MA60 且 MA20 > MA60  -> trend_up
       - 价格 < MA60 且 MA20 < MA60  -> trend_down
       - 其余(均线缠绕/横盘)          -> range
  3. 趋势强度不足时降级为 range(避免震荡市误判为趋势)。

指数加载器:
  - 优先用沪深300(000300): 通过腾讯日线接口抓取并缓存到 data/csi300_daily.parquet。
  - 抓取失败时(无网络/接口异常)回退到本地已有的上证指数(000001, kline_cache 中完整),
    并打印明确警告 —— 上证指数与沪深300 同为宽基, 市场状态高度一致, 可作本地回测代理。

用法:
  from tasks.market_state.01_regime_detector import RegimeDetector, load_index_series
  det = RegimeDetector()
  det.fit(close_series)              # 传入 pd.Series(index=date, values=close)
  states = det.detect(returns, vol, ...)  # 或在 detect_index(df) 内自动算
"""
from __future__ import annotations

import sys
import importlib.util
from pathlib import Path
from typing import Dict, Optional, Sequence

import numpy as np
import pandas as pd

ROOT = Path('.').resolve()

# 让本模块既能被脚本直接跑, 也能被 tasks 下的其他脚本 import
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CSI300_CODE = '000300'
FALLBACK_CODE = '000001'          # 上证指数(本地 kline_cache 完整)
CACHE_FILE = ROOT / 'data' / 'csi300_daily.parquet'


# --------------------------------------------------------------------------- #
# 指数日线加载(沪深300 优先, 上证指数回退)
# --------------------------------------------------------------------------- #
def _load_momentum_module():
    """按 CI 的方式以 importlib 加载项目根作为 momentum 包。失败返回 None。"""
    try:
        spec = importlib.util.spec_from_file_location(
            'momentum', ROOT / '__init__.py',
            submodule_search_locations=[str(ROOT)])
        m = importlib.util.module_from_spec(spec)
        sys.modules['momentum'] = m
        spec.loader.exec_module(m)
        return m
    except Exception:
        return None


def fetch_csi300_tencent(count: int = 900, max_retry: int = 3) -> Optional[pd.DataFrame]:
    """直接从腾讯日线接口抓取沪深300(000300)。返回含 trade_date/close/open/high/
    low/volume 的 DataFrame, 失败返回 None。

    注意: 显式禁用系统代理(本地常带 HTTP_PROXY, 经代理访问腾讯接口会被限流/返回空
    day 字段); CI 中腾讯接口本就直连, 此处 trust_env=False 与之行为一致。带重试。
    """
    import requests
    for attempt in range(1, max_retry + 1):
        try:
            s = requests.Session()
            s.trust_env = False           # 绕过本地代理, 直连腾讯
            prefix = 'sh' if CSI300_CODE.startswith('6') else 'sz'
            url = (f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
                   f'?param={prefix}{CSI300_CODE},day,,,{count},qfq')
            resp = s.get(url, headers={'Referer': 'https://gu.qq.com/'}, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            node = data.get('data', {}).get(f'{prefix}{CSI300_CODE}')
            if not node:
                continue
            day = node.get('day') or node.get('qfqday') or []
            if not day:
                continue
            rows = []
            for it in day:
                if len(it) < 6:
                    continue
                close = float(it[2]); vol = float(it[5]) * 100
                rows.append({'trade_date': it[0], 'open': float(it[1]), 'close': close,
                             'high': float(it[3]), 'low': float(it[4]),
                             'volume': vol, 'amount': vol * close})
            if not rows:
                continue
            return pd.DataFrame(rows)
        except Exception:
            continue
    return None


def _load_from_sqlite(code: str) -> Optional[pd.DataFrame]:
    """从本地 qlib_pro_v16.db 的 kline_cache 读指数日线(用于回退 / 离线)。

    直接用 sqlite3 取数再用 pandas 组装, 避免 pandas.read_sql_query 对裸
    DBAPI 连接的告警/兼容问题; 任何异常均返回 None(不阻断主流程)。
    """
    try:
        import sqlite3
        con = sqlite3.connect(str(ROOT / 'qlib_pro_v16.db'))
        cur = con.execute(
            "SELECT trade_date, open, close, high, low, volume FROM kline_cache "
            "WHERE code=? ORDER BY trade_date", (code,))
        rows = cur.fetchall()
        con.close()
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=['trade_date', 'open', 'close',
                                         'high', 'low', 'volume'])
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)
        return df
    except Exception:
        return None


def load_index_series(prefer: str = CSI300_CODE) -> pd.DataFrame:
    """加载宽基指数日线。

    顺序: 优先 000300 缓存/抓取 -> 失败回退 000001 本地库。
    返回 DataFrame(trade_date, open, close, high, low, volume) 升序。
    打印所用指数代码, 方便审计(回测日志可见用的是沪深300 还是回退的上证)。
    """
    # 1) 优先沪深300: 先看缓存
    if CACHE_FILE.exists():
        df = pd.read_parquet(CACHE_FILE)
        print(f'[regime] 使用缓存沪深300(000300): {len(df)} 行 '
              f'{df.trade_date.iloc[0]}~{df.trade_date.iloc[-1]}')
        return df.sort_values('trade_date').reset_index(drop=True)

    # 2) 尝试抓取沪深300
    df = fetch_csi300_tencent()
    if df is not None and not df.empty:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(CACHE_FILE, index=False)
        print(f'[regime] 抓取并缓存沪深300(000300): {len(df)} 行 '
              f'{df.trade_date.iloc[0]}~{df.trade_date.iloc[-1]}')
        return df.sort_values('trade_date').reset_index(drop=True)

    # 3) 回退上证指数(本地库完整)
    fb = _load_from_sqlite(FALLBACK_CODE)
    if fb is not None and not fb.empty:
        print(f'[regime] ⚠️ 沪深300(000300) 不可用, 回退上证指数(000001) '
              f'作为市场状态代理: {len(fb)} 行。两者同为宽基, 状态高度一致。')
        return fb.sort_values('trade_date').reset_index(drop=True)

    raise RuntimeError('无法加载任何宽基指数日线(沪深300 抓取失败且本地无上证指数)')


# --------------------------------------------------------------------------- #
# RegimeDetector
# --------------------------------------------------------------------------- #
def _rolling_quantile(s: pd.Series, window: int) -> pd.Series:
    """返回每个时点的滚动分位(0~1), 用序号法避免 scipy 依赖。"""
    out = np.full(len(s), np.nan)
    arr = s.values.astype(float)
    for i in range(window - 1, len(s)):
        w = arr[i - window + 1: i + 1]
        out[i] = (w <= arr[i]).mean()
    return pd.Series(out, index=s.index)


class RegimeDetector:
    """市场状态识别器。

    方法:
      fit(close)           用收盘序列拟合(预计算所有滚动指标, 存于 self.metrics)
      detect()             返回每交易日状态 Series
      detect_index(df)     直接吃指数日线 DataFrame, 内部算 close/returns/vol 后 detect
    状态判定顺序见模块 docstring。阈值由 config 注入(self.cfg)。
    """

    STATES = ('trend_up', 'trend_down', 'range', 'high_vol')

    def __init__(self, cfg: Optional[dict] = None):
        # 默认阈值(亦可被 config/regime_config.yaml 的 detector 段覆盖)
        self.cfg = dict(
            ma_short=20, ma_long=60,
            vol_window=20,
            high_vol_quantile=0.80,      # 波动率处于历史 80 分位以上 -> high_vol
            high_vol_abs=0.30,           # 或年化波动率 > 30% 直接 high_vol
            trend_strength_min=0.02,     # |price/MA60 - 1| 需超过此值才算有趋势
            adx_min=20,                  # (预留)趋势强度阈值
        )
        if cfg:
            self.cfg.update(cfg)
        self.metrics: Optional[pd.DataFrame] = None
        self._close: Optional[pd.Series] = None

    # -- 指标计算 ---------------------------------------------------------- #
    def _compute_metrics(self, close: pd.Series) -> pd.DataFrame:
        c = close.astype(float)
        cfg = self.cfg
        ms, ml = cfg['ma_short'], cfg['ma_long']
        vw = cfg['vol_window']
        ret = c.pct_change().fillna(0.0)
        # 20 日已实现波动率(年化): std(日收益) * sqrt(252)
        rv = ret.rolling(vw).std() * np.sqrt(252)
        ma_s = c.rolling(ms).mean()
        ma_l = c.rolling(ml).mean()
        # 趋势强度: 价格相对长期均线的偏离
        dev60 = c / ma_l - 1.0
        # 均线斜率(短期相对长期)
        slope = ma_s / ma_l - 1.0
        # 滚动波动率分位(用于 high_vol 判定)
        vol_q = _rolling_quantile(rv, max(vw * 3, ml))
        m = pd.DataFrame({
            'close': c,
            'ret': ret,
            'rv': rv,
            'ma_s': ma_s,
            'ma_l': ma_l,
            'dev60': dev60,
            'slope': slope,
            'vol_q': vol_q,
        })
        return m

    # -- 拟合 -------------------------------------------------------------- #
    def fit(self, close: pd.Series):
        self._close = pd.Series(close.values, index=pd.to_datetime(close.index))
        self.metrics = self._compute_metrics(self._close)
        return self

    # -- 判定 -------------------------------------------------------------- #
    def detect(self) -> pd.Series:
        if self.metrics is None:
            raise RuntimeError('请先调用 fit(close) 或 detect_index(df)')
        m = self.metrics
        cfg = self.cfg
        states = []
        for i in range(len(m)):
            if i < cfg['ma_long']:          # 预热不足 -> 默认震荡
                states.append('range')
                continue
            rv = m['rv'].iat[i]
            vq = m['vol_q'].iat[i]
            dev = m['dev60'].iat[i]
            slope = m['slope'].iat[i]
            # 1) 高波动优先
            is_high_vol = (np.isfinite(vq) and vq >= cfg['high_vol_quantile']) or \
                          (np.isfinite(rv) and rv >= cfg['high_vol_abs'])
            if is_high_vol:
                states.append('high_vol')
                continue
            # 2) 趋势方向
            if dev > cfg['trend_strength_min'] and slope > 0:
                states.append('trend_up')
            elif dev < -cfg['trend_strength_min'] and slope < 0:
                states.append('trend_down')
            else:
                states.append('range')
        out = pd.Series(states, index=m.index, name='regime')
        return out

    # -- 直接吃指数日线 ---------------------------------------------------- #
    def detect_index(self, df: pd.DataFrame) -> pd.Series:
        df = df.sort_values('trade_date')
        close = pd.Series(df['close'].values,
                          index=pd.to_datetime(df['trade_date'].values))
        self.fit(close)
        return self.detect()

    def summary(self, states: Optional[pd.Series] = None) -> Dict[str, int]:
        s = states if states is not None else self.detect()
        return {st: int((s == st).sum()) for st in self.STATES}


# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    df = load_index_series()
    det = RegimeDetector()
    st = det.detect_index(df)
    print('\n=== 市场状态分布(全样本) ===')
    for k, v in det.summary(st).items():
        print(f'  {k:10s}: {v}')
    print('\n=== 最近 12 个交易日 ===')
    print(st.tail(12).to_string())
