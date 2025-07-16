import pandas as pd

from backtesting.engine import Backtest
from backtesting.execution import Execution, FlatCommission, GaussianSlippage
from backtesting.portfolio import Portfolio
from backtesting.strategies.sma_crossover import SmaCrossover


def main():
    # Create a sample dataframe
    data = {
        "asset": ["AAPL"] * 100,
        "close": [100 + i + (i % 5) * 5 for i in range(100)],
    }
    df = pd.DataFrame(data)

    # Create the backtesting components
    strategy = SmaCrossover(short_window=10, long_window=30)
    portfolio = Portfolio(initial_cash=100000)
    execution = Execution(
        commission_model=FlatCommission(0.001),
        slippage_model=GaussianSlippage(0, 0.001),
    )

    # Run the backtest
    backtest = Backtest(strategy, portfolio, execution, df)
    backtest.run()

    # Print the results
    print("Backtest Results:")
    print(f"Final PnL: {backtest.results['pnl']:.2f}")
    backtest.to_json("backtest_results.json")
    print("Results saved to backtest_results.json")


if __name__ == "__main__":
    main()
