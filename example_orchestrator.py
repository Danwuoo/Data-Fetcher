import pandas as pd
from backtesting.orchestrator import Orchestrator
from backtesting.strategies.sma_crossover import SmaCrossover
from backtesting.portfolio import Portfolio
from backtesting.execution import Execution, FlatCommission, GaussianSlippage
from backtesting.performance import Performance
from data_processing.data_handler import DataHandler
from data_storage.storage_backend import HybridStorageManager


def main():
    # Create a sample dataframe
    data = pd.DataFrame({
        "asset": ["AAPL"] * 200,
        "close": [100 + i + (i % 5) * 5 for i in range(200)],
    })

    # Create the orchestrator
    storage_manager = HybridStorageManager({})
    data_handler = DataHandler(storage_manager)
    orchestrator = Orchestrator(
        data_handler=data_handler,
        strategy_cls=SmaCrossover,
        portfolio_cls=Portfolio,
        execution_cls=lambda: Execution(
            commission_model=FlatCommission(0.001),
            slippage_model=GaussianSlippage(0, 0.001),
        ),
        performance_cls=Performance,
    )

    # Run a walk-forward backtest
    walk_forward_config = {
        "run_id": "walk_forward_example",
        "walk_forward": {
            "train_period": 100,
            "test_period": 50,
            "step_size": 50,
        },
        "strategy_params": {"short_window": 10, "long_window": 30},
        "portfolio_params": {"initial_cash": 100000},
    }
    print("Running walk-forward backtest...")
    orchestrator.run(walk_forward_config, data)
    orchestrator.to_json("walk_forward_results.json")
    print("Walk-forward backtest complete. Results saved to walk_forward_results.json")

    # Run a CPCV backtest
    cpcv_config = {
        "run_id": "cpcv_example",
        "cpcv": {
            "n_splits": 10,
            "n_test_splits": 2,
            "embargo": 5,
        },
        "strategy_params": {"short_window": 10, "long_window": 30},
        "portfolio_params": {"initial_cash": 100000},
    }
    print("\nRunning CPCV backtest...")
    orchestrator.run(cpcv_config, data)
    orchestrator.to_json("cpcv_results.json")
    print("CPCV backtest complete. Results saved to cpcv_results.json")


if __name__ == "__main__":
    main()
