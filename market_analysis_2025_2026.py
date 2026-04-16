"""
A股市场深度分析 (2025-2026)
数据驱动的策略建议系统
"""

import sqlite3
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

# 设置中文字体
plt.rcParams["font.sans-serif"] = ["SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


class MarketDataProvider:
    """从腾讯API获取行情数据"""

    DB_PATH = "market_cache.db"

    @staticmethod
    def _init_db():
        conn = sqlite3.connect(MarketDataProvider.DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
                symbol TEXT, date TEXT, open REAL, close REAL, high REAL, low REAL, vol REAL,
                PRIMARY KEY (symbol, date)
            )
        """)
        conn.commit()
        conn.close()

    @staticmethod
    def fetch_tencent(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从腾讯API获取历史数据，支持缓存"""
        MarketDataProvider._init_db()

        # 先从缓存查询
        conn = sqlite3.connect(MarketDataProvider.DB_PATH)
        cached = pd.read_sql(
            f"SELECT * FROM quotes WHERE symbol=? AND date BETWEEN ? AND ? ORDER BY date",
            conn,
            params=(symbol, start_date, end_date),
        )
        conn.close()

        if not cached.empty:
            cached["date"] = pd.to_datetime(cached["date"])
            return cached.set_index("date")[["open", "close", "high", "low", "vol"]]

        # 缓存未命中，从腾讯新API获取（返回最近500条）
        try:
            prefix = "sh" if symbol.startswith("5") else "sz"
            url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={prefix}{symbol},day,,,500,qfq"
            headers = {"Referer": "https://gu.qq.com/"}

            resp = requests.get(url, headers=headers, timeout=10)
            data = resp.json()

            kline_key = f"{prefix}{symbol}"
            if kline_key not in data.get("data", {}):
                return pd.DataFrame()

            day_data = data["data"][kline_key].get("day", [])
            if not day_data:
                day_data = data["data"][kline_key].get("qfqday", [])

            if not day_data:
                return pd.DataFrame()

            rows = []
            for item in day_data:
                if len(item) >= 6:
                    try:
                        rows.append(
                            {
                                "symbol": symbol,
                                "date": item[0],
                                "open": float(item[1]),
                                "close": float(item[2]),
                                "high": float(item[3]),
                                "low": float(item[4]),
                                "vol": float(item[5]) * 100,  # 手 -> 股
                            }
                        )
                    except (ValueError, IndexError):
                        continue

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])

            # 按日期筛选
            df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]

            if not df.empty:
                # 保存到缓存
                conn = sqlite3.connect(MarketDataProvider.DB_PATH)
                df.to_sql("quotes", conn, if_exists="append", index=False)
                conn.close()

            return df.set_index("date")[["open", "close", "high", "low", "vol"]]

        except Exception as e:
            print(f"⚠️ 获取 {symbol} 数据失败: {e}")
            return pd.DataFrame()


class TechnicalAnalyzer:
    """技术指标计算"""

    @staticmethod
    def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 移动平均线
        df["MA5"] = df["close"].rolling(5).mean()
        df["MA20"] = df["close"].rolling(20).mean()
        df["MA60"] = df["close"].rolling(60).mean()
        df["MA120"] = df["close"].rolling(120).mean()
        df["MA250"] = df["close"].rolling(250).mean()

        # RSI (14日)
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df["RSI"] = 100 - (100 / (1 + rs))

        # MACD
        exp1 = df["close"].ewm(span=12).mean()
        exp2 = df["close"].ewm(span=26).mean()
        df["MACD"] = exp1 - exp2
        df["Signal"] = df["MACD"].ewm(span=9).mean()
        df["MACD_Hist"] = df["MACD"] - df["Signal"]

        # 波动率 (20日年化)
        df["Volatility"] = df["close"].pct_change().rolling(20).std() * np.sqrt(252)

        # 支撑阻力 (52周高低)
        df["High52W"] = df["high"].rolling(252).max()
        df["Low52W"] = df["low"].rolling(252).min()

        return df

    @staticmethod
    def calculate_hurst(price_series: pd.Series, lags: int = 100) -> float:
        """计算Hurst指数，判断趋势性/均值回归性"""
        returns = np.log(price_series / price_series.shift(1)).dropna()

        tau = []
        for lag in range(1, min(lags + 1, len(returns) // 2)):
            n = len(returns)
            mean_ret = returns.mean()
            Y = np.cumsum(returns - mean_ret)
            R = np.max(Y) - np.min(Y)
            S = np.std(returns, ddof=1)
            if S > 0:
                tau.append(R / S)

        # 拟合
        tau = np.array(tau)
        if len(tau) < 2:
            return 0.5
        lags_arr = np.arange(1, len(tau) + 1)
        poly = np.polyfit(np.log(lags_arr), np.log(tau), 1)
        return poly[0]

    @staticmethod
    def get_trend_status(df: pd.DataFrame) -> str:
        """判断趋势状态"""
        if df.empty or len(df) < 20:
            return "数据不足"

        ma5 = df["MA5"].iloc[-1]
        ma20 = df["MA20"].iloc[-1]
        ma60 = df["MA60"].iloc[-1]
        close = df["close"].iloc[-1]
        macd_hist = df["MACD_Hist"].iloc[-1]

        # 10根K线密度
        recent_volatility = df["close"].pct_change().tail(10).std()

        if ma5 > ma20 > ma60 and close > ma60:
            return "强上升趋势"
        elif ma5 < ma20 < ma60 and close < ma60:
            return "强下降趋势"
        elif abs(ma5 - ma20) / ma20 < 0.02 and recent_volatility > 0.003:
            return "震荡市"
        elif ma5 > ma20 > ma60:
            return "上升趋势"
        elif ma5 < ma20 < ma60:
            return "下降趋势"
        else:
            return "震荡偏强"

    @staticmethod
    def rate_for_grid_strategy(df: pd.DataFrame) -> float:
        """评分 0-100，数值越高越适合网格策略"""
        if df.empty or len(df) < 100:
            return 0

        hurst = TechnicalAnalyzer.calculate_hurst(df["close"])
        volatility = df["close"].pct_change().std() * np.sqrt(252)
        range_high = df["close"].tail(252).max()
        range_low = df["close"].tail(252).min()
        swing_ratio = (range_high - range_low) / (range_low) if range_low > 0 else 0

        # 评分逻辑
        # Hurst < 0.45 得分高（强均值回归），> 0.55 得分低（趋势强）
        hurst_score = max(0, 100 * (0.5 - hurst) / 0.05) if hurst < 0.5 else max(0, 100 * (hurst - 0.6) / 0.1)
        hurst_score = min(100, hurst_score)

        # 波动率 15%-40% 最好
        vol_score = 100 if 0.15 < volatility < 0.4 else max(0, 100 - abs(volatility - 0.275) / 0.275 * 100)

        # 摆幅 10%-30% 最好
        swing_score = 100 if 0.1 < swing_ratio < 0.3 else max(0, 100 - abs(swing_ratio - 0.2) / 0.2 * 100)

        final_score = hurst_score * 0.4 + vol_score * 0.3 + swing_score * 0.3
        return min(100, final_score)


class MarketForecaster:
    """市场趋势预测"""

    @staticmethod
    def predict_next_30days(df: pd.DataFrame) -> Dict:
        """预测未来30天走势"""
        if df.empty or len(df) < 60:
            return {}

        # 最近30天收益率
        ret_30 = (df["close"].iloc[-1] / df["close"].iloc[-30] - 1) * 100

        # 60天收益率
        ret_60 = (df["close"].iloc[-1] / df["close"].iloc[-60] - 1) * 100

        # 动量指标
        momentum = df["close"].tail(5).mean() - df["close"].tail(20).mean()

        # RSI 极值判断
        rsi = df["RSI"].iloc[-1]

        # MACD 信号
        macd_positive = df["MACD"].iloc[-1] > df["Signal"].iloc[-1]
        macd_trend = "上升" if macd_positive else "下降"

        # 支撑阻力
        support = df["Low52W"].iloc[-1]
        resistance = df["High52W"].iloc[-1]
        current = df["close"].iloc[-1]
        distance_to_resistance = ((resistance - current) / current) * 100
        distance_to_support = ((current - support) / support) * 100

        # 综合预测
        if ret_30 > 10 and rsi > 70:
            outlook = "超买，可能回调"
            confidence = 0.7
        elif ret_30 < -10 and rsi < 30:
            outlook = "超卖，可能反弹"
            confidence = 0.7
        elif macd_positive and momentum > 0:
            outlook = "继续上升"
            confidence = 0.6
        elif not macd_positive and momentum < 0:
            outlook = "继续下降"
            confidence = 0.6
        else:
            outlook = "震荡整理"
            confidence = 0.5

        return {
            "outlook": outlook,
            "confidence": confidence,
            "ret_30d": ret_30,
            "ret_60d": ret_60,
            "rsi": rsi,
            "macd_trend": macd_trend,
            "distance_to_resistance": distance_to_resistance,
            "distance_to_support": distance_to_support,
            "momentum": momentum,
        }

    @staticmethod
    def recommend_strategy(df: pd.DataFrame, symbol: str) -> Dict:
        """推荐策略"""
        if df.empty:
            return {}

        trend = TechnicalAnalyzer.get_trend_status(df)
        grid_score = TechnicalAnalyzer.rate_for_grid_strategy(df)
        forecast = MarketForecaster.predict_next_30days(df)

        # 策略选择逻辑
        if "上升趋势" in trend and forecast.get("outlook", "") in ["继续上升", "超买，可能回调"]:
            if grid_score < 40:
                strategy = "趋势跟踪（均线黄金叉买入，死叉卖出）"
                reason = f"强上升趋势，网格评分仅{grid_score:.0f}分，网格会频繁止损"
            else:
                strategy = "趋势跟踪优先，可搭配网格"
                reason = f"上升趋势中，网格评分{grid_score:.0f}分，可作为加仓工具"
        elif "下降趋势" in trend:
            strategy = "空头回补或观望"
            reason = f"下降趋势，网格评分{grid_score:.0f}分，不建议做多"
        elif "震荡" in trend:
            if grid_score > 70:
                strategy = "网格策略（推荐）"
                reason = f"纯震荡市，网格评分{grid_score:.0f}分，最优选择"
            else:
                strategy = "定投+网格组合"
                reason = f"震荡市但网格评分{grid_score:.0f}分，定投为主"
        else:
            if grid_score > 60:
                strategy = "轻仓网格试探"
                reason = f"不确定行情，网格评分{grid_score:.0f}分，可小额试仓"
            else:
                strategy = "观望等待信号"
                reason = f"行情不明确，网格评分{grid_score:.0f}分，暂不建议介入"

        return {
            "symbol": symbol,
            "strategy": strategy,
            "reason": reason,
            "grid_score": grid_score,
            "trend": trend,
            "forecast": forecast,
        }


class SectorAnalyzer:
    """板块分析"""

    # 代表性 ETF 映射
    ETF_MAP = {
        "510300": "沪深300（宽基）",
        "510050": "上证50（蓝筹）",
        "512480": "半导体（科技）",
        "159915": "创业板（成长）",
        "513050": "中概互联（消费+互联网）",
        "518880": "黄金ETF（避险）",
        "560000": "新兴产业（新能源+芯片）",
        "512690": "旅游ETF（消费服务）",
    }

    @staticmethod
    def analyze_all_sectors(start_date: str, end_date: str) -> pd.DataFrame:
        """对比所有板块表现"""
        provider = MarketDataProvider()
        results = []

        for symbol, name in SectorAnalyzer.ETF_MAP.items():
            print(f"  获取 {name}...", end=" ")
            try:
                df = provider.fetch_tencent(symbol, start_date, end_date)
                if df.empty:
                    print("❌")
                    continue

                df = TechnicalAnalyzer.calculate_indicators(df)
                recommendation = MarketForecaster.recommend_strategy(df, symbol)

                ret_ytd = (df["close"].iloc[-1] / df["close"].iloc[0] - 1) * 100
                ret_6m = None
                if len(df) > 120:
                    ret_6m = (df["close"].iloc[-1] / df["close"].iloc[-120] - 1) * 100

                volatility = df["close"].pct_change().std() * np.sqrt(252)
                hurst = TechnicalAnalyzer.calculate_hurst(df["close"])

                results.append(
                    {
                        "标的": name,
                        "代码": symbol,
                        "YTD收益%": ret_ytd,
                        "6月收益%": ret_6m,
                        "年化波动%": volatility * 100,
                        "Hurst指数": hurst,
                        "网格评分": recommendation.get("grid_score", 0),
                        "趋势": recommendation.get("trend", "未知"),
                        "推荐策略": recommendation.get("strategy", "观望"),
                    }
                )
                print("✓")
            except Exception as e:
                print(f"❌ ({e})")

        return pd.DataFrame(results).sort_values("YTD收益%", ascending=False)


def main():
    print("\n" + "=" * 80)
    print("🔍 A股市场深度分析 (2025-2026)")
    print("=" * 80)

    start_date = "2025-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n📊 分析周期: {start_date} ~ {end_date}")

    # 第一步：全板块对比分析
    print("\n📈 正在下载和分析全板块数据...")
    sector_df = SectorAnalyzer.analyze_all_sectors(start_date, end_date)

    print("\n" + "=" * 80)
    print("📊 板块表现排行榜")
    print("=" * 80)
    print(sector_df.to_string(index=False))

    # 第二步：详细分析推荐板块
    print("\n" + "=" * 80)
    print("🎯 分板块详细分析与策略推荐")
    print("=" * 80)

    provider = MarketDataProvider()

    for _, row in sector_df.iterrows():
        symbol = row["代码"]
        name = row["标的"]

        print(f"\n{'─' * 80}")
        print(f"【{name}】({symbol})")
        print(f"{'─' * 80}")

        try:
            df = provider.fetch_tencent(symbol, start_date, end_date)
            if df.empty:
                print("  数据获取失败")
                continue

            df = TechnicalAnalyzer.calculate_indicators(df)
            recommendation = MarketForecaster.recommend_strategy(df, symbol)
            forecast = recommendation.get("forecast", {})

            # 基本信息
            current_price = df["close"].iloc[-1]
            ma5 = df["MA5"].iloc[-1]
            ma20 = df["MA20"].iloc[-1]
            ma60 = df["MA60"].iloc[-1]

            print(f"  当前价格: {current_price:.2f}")
            print(f"  MA5/MA20/MA60: {ma5:.2f} / {ma20:.2f} / {ma60:.2f}")
            print(f"  趋势判断: {recommendation['trend']}")
            print(f"  网格策略适用度: {recommendation['grid_score']:.1f}/100")

            # 预测信息
            if forecast:
                print(f"\n  📌 30天预测:")
                print(f"    - 走势展望: {forecast['outlook']}")
                print(f"    - 信心度: {forecast['confidence']:.0%}")
                print(f"    - 30日收益: {forecast['ret_30d']:.2f}%")
                print(f"    - RSI: {forecast['rsi']:.1f}")
                print(f"    - MACD信号: {forecast['macd_trend']}")
                if forecast.get("distance_to_resistance", 0) > 0:
                    print(f"    - 距上挡: +{forecast['distance_to_resistance']:.1f}%")
                if forecast.get("distance_to_support", 0) > 0:
                    print(f"    - 距下挡: -{forecast['distance_to_support']:.1f}%")

            # 策略推荐
            print(f"\n  🎯 策略推荐: {recommendation['strategy']}")
            print(f"  原因: {recommendation['reason']}")

        except Exception as e:
            print(f"  分析失败: {e}")

    # 第三步：复合分析
    print("\n" + "=" * 80)
    print("🔬 市场复合分析与综合建议")
    print("=" * 80)

    # 市场整体趋势
    print("\n【市场整体风格】")
    winners = sector_df[sector_df["YTD收益%"] > 10]
    losers = sector_df[sector_df["YTD收益%"] < -5]

    print(f"  表现最好5只: {', '.join(sector_df.head(5)['标的'].tolist())}")
    print(f"  表现最差5只: {', '.join(sector_df.tail(5)['标的'].tolist())}")

    # 高网格评分的标的
    high_grid = sector_df[sector_df["网格评分"] > 60].sort_values("网格评分", ascending=False)
    if not high_grid.empty:
        print(f"\n【网格策略最优目标】")
        print(f"  {high_grid.iloc[0]['标的']} (网格分: {high_grid.iloc[0]['网格评分']:.0f})")
        if len(high_grid) > 1:
            print(f"  {high_grid.iloc[1]['标的']} (网格分: {high_grid.iloc[1]['网格评分']:.0f})")

    # 平衡收益和波动的标的
    print(f"\n【风险调整最优标的】")
    sector_df["风险调整收益"] = sector_df["YTD收益%"] / (sector_df["年化波动%"] + 0.01)
    best_risk_adj = sector_df.loc[sector_df["风险调整收益"].idxmax()]
    print(f"  {best_risk_adj['标的']} (Sharpe: {best_risk_adj['风险调整收益']:.2f})")

    # 总体建议
    print(f"\n【总体策略建议】")
    avg_return = sector_df["YTD收益%"].mean()
    volatility_mean = sector_df["年化波动%"].mean()

    if avg_return > 15:
        print(f"  ✓ 市场处于明显牛市阶段（平均收益 {avg_return:.1f}%）")
        print(f"  ✓ 建议: 趋势跟踪为主，不建议大规模用网格")
        print(f"  ✓ 选标的: 优先选择高网格分数的标的形成网格底仓，搭配趋势追踪")
    elif -10 < avg_return < 10:
        print(f"  → 市场处于震荡阶段（平均收益 {avg_return:.1f}%）")
        print(f"  → 建议: 网格与趋势跟踪结合")
        if not high_grid.empty:
            print(f"  → 网格优先选择: {high_grid.iloc[0]['标的']}")
    else:
        print(f"  ✗ 市场处于明显熊市阶段（平均收益 {avg_return:.1f}%）")
        print(f"  ✗ 建议: 空头回补，暂不建议开新仓")

    print(f"\n  波动率平均: {volatility_mean:.1f}% (高={volatility_mean > 25})")

    print("\n" + "=" * 80)
    print("✅ 分析完成")
    print("=" * 80 + "\n")

    return sector_df


if __name__ == "__main__":
    main()
