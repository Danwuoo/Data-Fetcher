import pandas as pd

from backtest_data_module.backtesting.orchestrator import Orchestrator
from backtest_data_module.backtesting.strategies.sma_crossover import SmaCrossover
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.execution import Execution, FlatCommission, GaussianSlippage
from backtest_data_module.backtesting.performance import Performance
from backtest_data_module.data_handler import DataHandler
from backtest_data_module.data_storage.storage_backend import HybridStorageManager
from backtest_data_module.strategy_manager.registry import strategy_registry


def main():
    # Create a sample dataframe
    data = pd.DataFrame({
        "date": pd.to_datetime(pd.date_range("2022-01-01", periods=200)),
        "asset": ["AAPL"] * 200,
        "close": [100 + i + (i % 5) * 5 for i in range(200)],
    }).set_index("date")

    # Register the strategy
    strategy_registry.register("SmaCrossover", SmaCrossover)

    # Create the orchestrator
    storage_manager = HybridStorageManager({})
    data_handler = DataHandler(storage_manager)
    orchestrator = Orchestrator(
        data_handler=data_handler,
        strategy_name="SmaCrossover",
        portfolio_cls=Portfolio,
        execution_cls=Execution,
        performance_cls=Performance,
    )

    # Run a walk-forward backtest
    walk_forward_config = {
        "walk_forward": {
            "train_period": 100,
            "test_period": 50,
            "step_size": 50,
        },
        "strategy_params": {"short_window": 10, "long_window": 30},
        "portfolio_params": {"initial_cash": 100000},
        "execution_params": {
            "commission_model": FlatCommission(0.001),
            "slippage_model": GaussianSlippage(0, 0.001),
        },
    }
    print("Running walk-forward backtest...")
    orchestrator.run_ray(walk_forward_config, data)
    orchestrator.to_json("walk_forward_results.json")
    orchestrator.generate_reports(output_dir="examples/outputs")
    print("Walk-forward backtest complete. Results saved to walk_forward_results.json")

    # Run a CPCV backtest
    cpcv_config = {
        "cpcv": {
            "N": 10,
            "k": 2,
            "embargo_pct": 0.05,
        },
        "strategy_params": {"short_window": 10, "long_window": 30},
        "portfolio_params": {"initial_cash": 100000},
        "execution_params": {
            "commission_model": FlatCommission(0.001),
            "slippage_model": GaussianSlippage(0, 0.001),
        },
    }
    print("\nRunning CPCV backtest...")
    orchestrator.run_ray(cpcv_config, data)
    orchestrator.to_json("cpcv_results.json")
    orchestrator.generate_reports(output_dir="examples/outputs")
    print("CPCV backtest complete. Results saved to cpcv_results.json")


if __name__ == "__main__":
    main()
