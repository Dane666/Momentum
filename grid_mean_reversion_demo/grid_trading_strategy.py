"""
风险提示：
1) 网格策略属于均值回归策略，在单边趋势行情（持续上涨/持续下跌）中可能持续亏损。
2) 本示例仅用于教学与研究，请先模拟盘测试，并结合严格止损和仓位管理。
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class TradeRecord:
    date: Any
    trade_type: str
    price: float
    quantity: int
    commission: float
    stamp_duty: float
    cash_after: float
    position_after: int
    total_asset_after: float
    pnl: Optional[float] = None


class DataFetcher:
    """数据获取模块：负责下载并清洗日线数据。"""

    @staticmethod
    def fetch(symbol: str, start_date: str, end_date: str, ma_window: int) -> pd.DataFrame:
        data = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False, progress=False)
        if data.empty:
            raise ValueError(f"未获取到数据，请检查标的代码和日期区间：{symbol}")

        data = data.rename(columns={"Open": "open", "Close": "close", "Volume": "volume"})
        keep_cols = [c for c in ["open", "close", "volume"] if c in data.columns]
        data = data[keep_cols].copy()

        # 处理缺失值：价格缺失行删除，成交量缺失填0
        data = data.dropna(subset=["close"])
        if "open" in data.columns:
            data["open"] = data["open"].fillna(data["close"])
        if "volume" in data.columns:
            data["volume"] = data["volume"].fillna(0)

        # 计算均线作为中枢候选
        data["ma"] = data["close"].rolling(ma_window).mean()
        data = data.dropna(subset=["ma"])

        data.index = pd.to_datetime(data.index)
        data = data.sort_index()
        return data


class GridTradingStrategy:
    def __init__(
        self,
        symbol: str = "510300.SS",
        start_date: str = "2020-01-01",
        end_date: str = "2023-12-31",
        init_capital: float = 100000,
        grid_num: int = 10,
        grid_spacing: float = 0.02,
        trade_unit: int = 100,
        stop_loss_pct: float = 0.1,
        ma_window: int = 20,
        commission_rate: float = 0.00025,
        stamp_duty_rate: float = 0.001,
        max_position_ratio: float = 0.8,
        use_ma_center: bool = True,
    ):
        if grid_num <= 0 or grid_num % 2 != 0:
            raise ValueError("grid_num 必须是正偶数（如10，表示上下各5层）")
        if grid_spacing <= 0:
            raise ValueError("grid_spacing 必须大于0")
        if trade_unit <= 0:
            raise ValueError("trade_unit 必须大于0")

        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.init_capital = float(init_capital)
        self.grid_num = int(grid_num)
        self.grid_spacing = float(grid_spacing)
        self.trade_unit = int(trade_unit)
        self.stop_loss_pct = float(stop_loss_pct)
        self.ma_window = int(ma_window)
        self.commission_rate = float(commission_rate)
        self.stamp_duty_rate = float(stamp_duty_rate)
        self.max_position_ratio = float(max_position_ratio)
        self.use_ma_center = use_ma_center

        self.data: Optional[pd.DataFrame] = None
        self.center_price: Optional[float] = None
        self.grid_levels: Dict[str, List[float]] = {}

        self.trade_records: List[TradeRecord] = []
        self.equity_curve: Optional[pd.DataFrame] = None
        self.performance: Optional[pd.Series] = None

    def calculate_grid_levels(self, center_price: float) -> Dict[str, List[float]]:
        """根据中枢价计算上下网格线。"""
        half = self.grid_num // 2
        buy_levels = [center_price * (1 - self.grid_spacing * k) for k in range(1, half + 1)]
        sell_levels = [center_price * (1 + self.grid_spacing * k) for k in range(1, half + 1)]

        self.center_price = center_price
        self.grid_levels = {
            "buy_levels": sorted(buy_levels),       # 从低到高
            "sell_levels": sorted(sell_levels),     # 从低到高
            "all_levels": sorted(buy_levels + [center_price] + sell_levels),
        }
        return self.grid_levels

    def _price_to_grid_index(self, price: float) -> int:
        """
        将价格映射到网格索引：
        0 代表中心附近区间；
        -1, -2 ... 代表向下跌破的层数；
        +1, +2 ... 代表向上突破的层数。
        """
        half = self.grid_num // 2
        center = self.center_price
        if center is None:
            raise ValueError("center_price 未初始化")

        index = 0
        for k in range(1, half + 1):
            down_level = center * (1 - self.grid_spacing * k)
            up_level = center * (1 + self.grid_spacing * k)
            if price <= down_level:
                index = -k
            if price >= up_level:
                index = k
        return index

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """遍历价格序列，按网格穿越生成买卖信号。"""
        if self.center_price is None:
            raise ValueError("请先调用 calculate_grid_levels()")

        signals = []
        prev_idx = self._price_to_grid_index(data["close"].iloc[0])
        min_buy_level = min(self.grid_levels["buy_levels"])
        stop_loss_line = min_buy_level * (1 - self.stop_loss_pct)
        stop_loss_triggered = False

        for dt, row in data.iterrows():
            price = float(row["close"])
            curr_idx = self._price_to_grid_index(price)

            # 触发止损：跌破最下方网格线的 stop_loss_pct，标记后续不再开新仓
            if (not stop_loss_triggered) and price <= stop_loss_line:
                signals.append(
                    {
                        "date": dt,
                        "signal": "stop_loss",
                        "price": price,
                        "quantity": 0,
                        "grid_change": 0,
                    }
                )
                stop_loss_triggered = True
                prev_idx = curr_idx
                continue

            # 止损后不再产生新开仓信号（可扩展为重建网格后继续）
            if stop_loss_triggered:
                prev_idx = curr_idx
                continue

            grid_change = curr_idx - prev_idx
            if grid_change < 0:
                # 向下穿越网格，买入
                signals.append(
                    {
                        "date": dt,
                        "signal": "buy",
                        "price": price,
                        "quantity": abs(grid_change) * self.trade_unit,
                        "grid_change": grid_change,
                    }
                )
            elif grid_change > 0:
                # 向上穿越网格，卖出
                signals.append(
                    {
                        "date": dt,
                        "signal": "sell",
                        "price": price,
                        "quantity": abs(grid_change) * self.trade_unit,
                        "grid_change": grid_change,
                    }
                )

            prev_idx = curr_idx

        signal_df = pd.DataFrame(signals)
        if not signal_df.empty:
            signal_df["date"] = pd.to_datetime(signal_df["date"])
        return signal_df

    def _calc_fee(self, trade_type: str, price: float, quantity: int) -> Dict[str, float]:
        turnover = price * quantity
        commission = turnover * self.commission_rate
        stamp_duty = turnover * self.stamp_duty_rate if trade_type == "sell" else 0.0
        return {"commission": commission, "stamp_duty": stamp_duty}

    def execute_trades(self, signals: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
        """根据信号模拟交易，更新现金、仓位、总资产并记录明细。"""
        cash = self.init_capital
        position = 0
        holding_cost = 0.0  # 持仓总成本（用于计算已实现盈亏）

        signal_map = {}
        if not signals.empty:
            for _, sig in signals.iterrows():
                signal_map.setdefault(sig["date"], []).append(sig)

        equity_rows = []
        self.trade_records = []
        realized_pnls = []

        for dt, row in data.iterrows():
            price = float(row["close"])

            day_signals = signal_map.get(dt, [])
            for sig in day_signals:
                sig_type = sig["signal"]
                qty = int(sig["quantity"])

                if sig_type == "stop_loss":
                    if position > 0:
                        qty = position
                        fee = self._calc_fee("sell", price, qty)
                        proceeds = price * qty - fee["commission"] - fee["stamp_duty"]

                        avg_cost = holding_cost / position if position > 0 else 0.0
                        trade_pnl = proceeds - avg_cost * qty
                        realized_pnls.append(trade_pnl)

                        cash += proceeds
                        position -= qty
                        holding_cost = 0.0

                        self.trade_records.append(
                            TradeRecord(
                                date=dt,
                                trade_type="stop_loss_sell",
                                price=price,
                                quantity=qty,
                                commission=fee["commission"],
                                stamp_duty=fee["stamp_duty"],
                                cash_after=cash,
                                position_after=position,
                                total_asset_after=cash + position * price,
                                pnl=trade_pnl,
                            )
                        )
                    continue

                if sig_type == "buy":
                    # 仓位上限：最多持有 max_position_ratio 对应市值
                    max_position_shares = int((self.init_capital * self.max_position_ratio) / price)
                    allowed_buy = max(0, max_position_shares - position)
                    qty = min(qty, allowed_buy)

                    # 资金上限：现金不够则缩减下单量（按100股整数倍）
                    if qty > 0:
                        fee = self._calc_fee("buy", price, qty)
                        required_cash = price * qty + fee["commission"]

                        if required_cash > cash:
                            # 重新计算可买数量
                            raw_qty = int(cash / (price * (1 + self.commission_rate)))
                            raw_qty = max(0, raw_qty)
                            qty = (raw_qty // self.trade_unit) * self.trade_unit

                    if qty <= 0:
                        continue

                    fee = self._calc_fee("buy", price, qty)
                    cost = price * qty + fee["commission"]
                    if cost > cash:
                        continue

                    cash -= cost
                    position += qty
                    holding_cost += price * qty + fee["commission"]

                    self.trade_records.append(
                        TradeRecord(
                            date=dt,
                            trade_type="buy",
                            price=price,
                            quantity=qty,
                            commission=fee["commission"],
                            stamp_duty=0.0,
                            cash_after=cash,
                            position_after=position,
                            total_asset_after=cash + position * price,
                            pnl=None,
                        )
                    )

                if sig_type == "sell":
                    qty = min(qty, position)
                    if qty <= 0:
                        continue

                    fee = self._calc_fee("sell", price, qty)
                    proceeds = price * qty - fee["commission"] - fee["stamp_duty"]

                    avg_cost = holding_cost / position if position > 0 else 0.0
                    trade_pnl = proceeds - avg_cost * qty
                    realized_pnls.append(trade_pnl)

                    cash += proceeds
                    position -= qty
                    holding_cost -= avg_cost * qty
                    if position == 0:
                        holding_cost = 0.0

                    self.trade_records.append(
                        TradeRecord(
                            date=dt,
                            trade_type="sell",
                            price=price,
                            quantity=qty,
                            commission=fee["commission"],
                            stamp_duty=fee["stamp_duty"],
                            cash_after=cash,
                            position_after=position,
                            total_asset_after=cash + position * price,
                            pnl=trade_pnl,
                        )
                    )

            total_asset = cash + position * price
            equity_rows.append(
                {
                    "date": dt,
                    "close": price,
                    "cash": cash,
                    "position": position,
                    "total_asset": total_asset,
                }
            )

        equity_df = pd.DataFrame(equity_rows).set_index("date")
        equity_df["strategy_nav"] = equity_df["total_asset"] / self.init_capital
        equity_df["benchmark_nav"] = equity_df["close"] / equity_df["close"].iloc[0]
        equity_df["drawdown"] = equity_df["strategy_nav"] / equity_df["strategy_nav"].cummax() - 1

        self.equity_curve = equity_df
        self._realized_pnls = realized_pnls
        return equity_df

    def evaluate_performance(self, risk_free_rate: float = 0.03) -> pd.Series:
        if self.equity_curve is None:
            raise ValueError("请先执行 execute_trades()")

        nav = self.equity_curve["strategy_nav"]
        daily_ret = nav.pct_change().dropna()

        total_return = nav.iloc[-1] - 1
        annualized_return = (1 + total_return) ** (252 / max(1, len(daily_ret))) - 1
        max_drawdown = self.equity_curve["drawdown"].min()

        if daily_ret.std() > 0:
            sharpe = ((daily_ret.mean() - risk_free_rate / 252) / daily_ret.std()) * np.sqrt(252)
        else:
            sharpe = np.nan

        realized = pd.Series(self._realized_pnls) if hasattr(self, "_realized_pnls") else pd.Series(dtype=float)
        win_rate = (realized > 0).mean() if not realized.empty else np.nan

        perf = pd.Series(
            {
                "累计收益率": total_return,
                "年化收益率": annualized_return,
                "最大回撤": max_drawdown,
                "夏普比率(3%无风险)": sharpe,
                "胜率(按已平仓卖出)": win_rate,
                "交易次数": len(self.trade_records),
            }
        )
        self.performance = perf
        return perf

    def plot_results(self):
        if self.data is None or self.equity_curve is None:
            raise ValueError("请先运行回测")

        fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

        # 1) 价格+网格线+买卖点
        ax1 = axes[0]
        ax1.plot(self.data.index, self.data["close"], label="收盘价", color="black", linewidth=1.2)

        if self.center_price is not None:
            ax1.axhline(self.center_price, color="blue", linestyle="--", alpha=0.8, label="中枢价")
            for lv in self.grid_levels.get("buy_levels", []):
                ax1.axhline(lv, color="green", linestyle=":", alpha=0.45)
            for lv in self.grid_levels.get("sell_levels", []):
                ax1.axhline(lv, color="red", linestyle=":", alpha=0.45)

        buys = [r for r in self.trade_records if r.trade_type == "buy"]
        sells = [r for r in self.trade_records if r.trade_type in ("sell", "stop_loss_sell")]

        if buys:
            ax1.scatter([r.date for r in buys], [r.price for r in buys], marker="^", color="green", s=40, label="买点")
        if sells:
            ax1.scatter([r.date for r in sells], [r.price for r in sells], marker="v", color="red", s=40, label="卖点")

        ax1.set_title(f"{self.symbol} 价格走势 + 网格线 + 买卖点")
        ax1.legend(loc="best")
        ax1.grid(alpha=0.25)

        # 2) 策略净值 vs 标的净值
        ax2 = axes[1]
        ax2.plot(self.equity_curve.index, self.equity_curve["strategy_nav"], label="策略净值", color="blue")
        ax2.plot(self.equity_curve.index, self.equity_curve["benchmark_nav"], label="标的净值", color="orange")
        ax2.set_title("策略净值 vs 标的净值")
        ax2.legend(loc="best")
        ax2.grid(alpha=0.25)

        # 3) 回撤曲线
        ax3 = axes[2]
        ax3.fill_between(
            self.equity_curve.index,
            self.equity_curve["drawdown"],
            0,
            color="red",
            alpha=0.35,
            label="回撤",
        )
        ax3.set_title("回撤曲线")
        ax3.legend(loc="best")
        ax3.grid(alpha=0.25)

        plt.tight_layout()
        plt.show()

    def backtest(self) -> Dict[str, Any]:
        self.data = DataFetcher.fetch(
            symbol=self.symbol,
            start_date=self.start_date,
            end_date=self.end_date,
            ma_window=self.ma_window,
        )

        # 中枢价：可选“首个有效均线”或“首日收盘价”
        center = float(self.data["ma"].iloc[0]) if self.use_ma_center else float(self.data["close"].iloc[0])
        self.calculate_grid_levels(center)

        signals = self.generate_signals(self.data)
        equity = self.execute_trades(signals, self.data)
        perf = self.evaluate_performance(risk_free_rate=0.03)

        trades_df = pd.DataFrame([t.__dict__ for t in self.trade_records])
        if not trades_df.empty:
            trades_df = trades_df.sort_values("date").reset_index(drop=True)

        return {
            "data": self.data,
            "signals": signals,
            "equity": equity,
            "trades": trades_df,
            "performance": perf,
        }

    # 预留扩展接口（保持简单，不在本示例实现复杂逻辑）
    def adjust_grid_dynamically(self):
        """预留：可在此实现动态网格（如波动率自适应网格间距）。"""
        pass

    def rotate_multi_assets(self):
        """预留：可在此实现多标的轮动逻辑。"""
        pass


if __name__ == "__main__":
    # 示例配置（可直接修改，不需要改核心逻辑）
    config = {
        "symbol": "510300.SS",
        "start_date": "2020-01-01",
        "end_date": "2023-12-31",
        "init_capital": 100000,
        "grid_num": 10,
        "grid_spacing": 0.02,
        "trade_unit": 100,
        "stop_loss_pct": 0.10,
        "ma_window": 20,
        "commission_rate": 0.00025,
        "stamp_duty_rate": 0.001,
        "max_position_ratio": 0.8,
        "use_ma_center": True,
    }

    strategy = GridTradingStrategy(**config)
    result = strategy.backtest()

    print("\n=== 策略绩效报告 ===")
    perf = result["performance"].copy()
    for key in ["累计收益率", "年化收益率", "最大回撤", "胜率(按已平仓卖出)"]:
        if key in perf and pd.notna(perf[key]):
            perf[key] = f"{perf[key]:.2%}"
    if "夏普比率(3%无风险)" in perf and pd.notna(perf["夏普比率(3%无风险)"]):
        perf["夏普比率(3%无风险)"] = f"{perf['夏普比率(3%无风险)']:.3f}"
    print(perf.to_frame(name="数值"))

    trades = result["trades"]
    if not trades.empty:
        print("\n=== 最近5笔交易 ===")
        print(trades.tail(5))
    else:
        print("\n本区间未触发交易，请尝试调整 grid_spacing 或回测区间。")

    strategy.plot_results()
