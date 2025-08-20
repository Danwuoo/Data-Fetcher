# -*- coding: utf-8 -*-

"""
這是一個關於如何在本回測框架中撰寫並執行策略的完整指南 (修正版)。
This is a complete guide (revised) on how to write and run a strategy in this backtesting framework.
"""

import polars as pl
from typing import List
from datetime import date, timedelta

# 核心模組匯入
from backtest_data_module.backtesting.strategy import StrategyBase
from backtest_data_module.backtesting.events import SignalEvent
# from backtest_data_module.backtesting.engine import Backtest # HACK: Bypassed
from backtest_data_module.backtesting.execution import Execution, FlatCommission
from backtest_data_module.backtesting.portfolio import Portfolio
# from backtest_data_module.backtesting.performance import Performance # HACK: Bypassed


# =====================================================================================
# 步驟 1: 新增您的技術指標
#
# 在這個框架中，新增技術指標最簡單的方式就是編寫一個函式。
# 這個函式接收一個 Polars Series (例如收盤價)，並回傳一個計算好的指標 Series。
# =====================================================================================


def calculate_rsi(close_prices_expr: pl.Expr, window: int = 14) -> pl.Expr:
    """
    使用 Polars 表達式計算相對強弱指數 (RSI)。
    這樣可以讓函式在 `with_columns` 中直接使用。
    """
    # 計算價格變動
    delta = close_prices_expr.diff()

    # 分別計算上漲和下跌 (使用 `when/then/otherwise` 以相容 Expr)
    gain = pl.when(delta > 0).then(delta).otherwise(0)
    loss = pl.when(delta < 0).then(-delta).otherwise(0)

    # 使用指數移動平均 (Exponential Moving Average) 來平滑漲跌值
    avg_gain = gain.ewm_mean(span=window, adjust=False)
    avg_loss = loss.ewm_mean(span=window, adjust=False)

    # 計算相對強度 (RS)
    rs = avg_gain / avg_loss

    # 計算 RSI
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fill_null(50) # 初始值填 50 (中性)

# =====================================================================================
# 步驟 2: 在您的策略中使用新指標
# =====================================================================================

class StrategyWithRSI(StrategyBase):
    """
    一個結合了均線交叉和 RSI 指標的策略。
    - 買入條件: 短期均線上穿長期均線 AND RSI < 70 (避免在超買區買入)。
    - 賣出條件: 短期均線下穿長期均線 AND RSI > 30 (避免在超賣區賣出)。
    """
    def __init__(self, short_window: int = 10, long_window: int = 30, rsi_window: int = 14):
        super().__init__({})
        self.short_window = short_window
        self.long_window = long_window
        self.rsi_window = rsi_window
        print(f"策略初始化：短均線={self.short_window}, 長均線={self.long_window}, RSI週期={self.rsi_window}")

    def on_data(self, data: pl.DataFrame) -> List[SignalEvent]:
        signals = []
        for asset_name in data['asset'].unique():
            asset_data = data.filter(pl.col('asset') == asset_name)

            # --- 計算所有指標 ---
            df_indicators = asset_data.with_columns(
                pl.col("close").rolling_mean(
                    window_size=self.short_window
                ).alias("short_sma"),
                pl.col("close").rolling_mean(
                    window_size=self.long_window
                ).alias("long_sma"),
                calculate_rsi(
                    pl.col("close"), window=self.rsi_window
                ).alias("rsi")
            )

            # --- 訊號產生邏輯 ---
            df_signal = df_indicators.with_columns(
                pl.when(
                    (pl.col('short_sma') > pl.col('long_sma')) & (pl.col('rsi') < 70)
                ).then(1)
                .when(
                    (pl.col('short_sma') < pl.col('long_sma')) & (pl.col('rsi') > 30)
                ).then(-1)
                .otherwise(0)
                .alias('position')
            )

            df_trade = df_signal.with_columns(
                pl.col('position').diff().fill_null(0).alias('trade_signal')
            )

            for row in df_trade.filter(pl.col('trade_signal') != 0).iter_rows(named=True):
                trade_signal = row['trade_signal']
                if trade_signal > 0:
                    direction = 'long'
                    quantity = 10
                else:
                    direction = 'short'
                    quantity = -10

                print(
                    f"時間: {row['date']}, 資產: {row['asset']}, "
                    f"RSI: {row['rsi']:.2f} - 產生 {direction} 訊號"
                )
                signals.append(
                    SignalEvent(
                        asset=row['asset'],
                        quantity=quantity,
                        direction=direction
                    )
                )
        return signals

# =====================================================================================
# 步驟 3: 設定並執行回測
# =====================================================================================

def run_backtest_workaround():
    print("\n--- 開始執行回測 (含RSI指標) ---")

    # 1. 準備資料
    base_date = date(2023, 1, 1)
    dates = [(base_date + timedelta(days=i)) for i in range(100)]
    data = {
        "asset": ["TEST_STOCK"] * 100,
        "close": [50 + i + (i % 7 - 3) * 2 for i in range(100)],
        "date": dates,
        "timestamp": range(100)
    }
    df = pl.DataFrame(data)
    print(f"成功建立範例資料，共 {len(df)} 筆。")

    # 2. 實例化元件 (使用新策略)
    initial_cash = 10000.0
    strategy = StrategyWithRSI(short_window=5, long_window=20, rsi_window=14)
    portfolio = Portfolio(initial_cash=initial_cash)
    execution = Execution(commission_model=FlatCommission(fee=0.001))

    # 3. 執行策略並取得訊號
    signals = strategy.on_data(df)

    # 4. 模擬執行
    for signal in signals:
        # 這裡的執行邏輯保持不變，因為它只關心最終的訊號
        asset_data = df.filter(pl.col("asset") == signal.asset)
        last_date_for_asset = asset_data['date'].max()
        price_on_date = asset_data.filter(pl.col('date') == last_date_for_asset)['close'][0]

        fill_event = {
            "asset": signal.asset,
            "quantity": signal.quantity,
            "price": price_on_date,
            "commission": execution.commission_model.calculate(signal.quantity, price_on_date)
        }
        portfolio.update([fill_event])

    # 5. 計算結果
    last_prices = df.group_by("asset").last().select(["asset", "close"]).to_dict()
    last_prices_map = {a: p for a, p in zip(last_prices["asset"], last_prices["close"])}

    pnl = portfolio.get_pnl(last_prices_map)
    final_equity = initial_cash + pnl

    print("\n--- 回測結果 (含RSI指標) ---")
    print(f"最終資產淨值: {final_equity:.2f}")
    print(f"總收益/虧損: {pnl:.2f}")
    print(f"總交易次數: {len(portfolio.fills)}")


if __name__ == "__main__":
    run_backtest_workaround()
