import polars as pl

from backtest_data_module.backtesting.engine import Backtest
from backtest_data_module.backtesting.execution import (
    Execution,
    FlatCommission,
    GaussianSlippage,
)
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.strategies.sma_crossover import SmaCrossover
from backtest_data_module.backtesting.performance import Performance


def main():
    # Create a sample dataframe
    data = {
        "asset": ["AAPL"] * 100,
        "close": [100 + i + (i % 5) * 5 for i in range(100)],
        "timestamp": range(100),
    }
    df = pl.DataFrame(data)

    # Create the backtesting components
    strategy = SmaCrossover(params={"short_window": 10, "long_window": 30})
    portfolio = Portfolio(initial_cash=100000)
    execution = Execution(
        commission_model=FlatCommission(0.001),
        slippage_model=GaussianSlippage(0, 0.001),
    )
    performance = Performance()

    # Run the backtest
    backtest = Backtest(strategy, portfolio, execution, performance, df)
    backtest.run()

    # Print the results
    print("Backtest Results:")
    print(f"Final PnL: {backtest.results['pnl']:.2f}")
    backtest.to_json("backtest_results.json")
    print("Results saved to backtest_results.json")


if __name__ == "__main__":
    main()
