# Plugin Architecture

The backtesting framework uses a plugin-style architecture to load custom strategies. This approach decouples the core framework from user-defined strategies, making it easy to extend and maintain.

## Strategy Registry

The central component of the plugin system is the `StrategyRegistry`, located in `src/backtest_data_module/strategy_manager/registry.py`. This class is responsible for discovering, registering, and loading strategies.

### Registration

Strategies can be registered in two ways:

1.  **Manual Registration**: You can explicitly register a strategy using the `register` method of the `strategy_registry` instance.

    ```python
    from backtest_data_module.strategy_manager.registry import strategy_registry
    from my_strategies import MyAwesomeStrategy

    strategy_registry.register("MyAwesomeStrategy", MyAwesomeStrategy)
    ```

2.  **Automatic Discovery**: The `discover` method can automatically find and register strategies from a given directory. The registry will search for modules, import them, and register any class that inherits from `StrategyBase`.

    ```python
    from backtest_data_module.strategy_manager.registry import strategy_registry

    strategy_registry.discover("path/to/my/strategies")
    ```

### Usage

Once a strategy is registered, you can instantiate the `Orchestrator` with the strategy's name.

```python
from backtest_data_module.backtesting.orchestrator import Orchestrator

orchestrator = Orchestrator(
    data_handler=data_handler,
    strategy_name="MyAwesomeStrategy",
    # ... other parameters
)
```

## Core Module APIs

This section provides an overview of the APIs for the core modules of the backtesting framework.

### Strategy

-   **`StrategyBase`**: The abstract base class for all strategies.
    -   `on_data(event)`: This abstract method must be implemented by all concrete strategies. It is called for each new data event and should return a list of `SignalEvent`s.
    -   `on_start(context)`: Called at the beginning of a backtest.
    -   `on_finish(context)`: Called at the end of a backtest.

### DataHandler

-   **`DataHandler`**: The centralized interface for data access.
    -   `read(query, tiers, compressed_cols)`: Reads data from the specified storage tiers.
    -   `stream(symbols, freq)`: Streams data asynchronously.

### Execution

-   **`Execution`**: Handles order execution.
    -   `place_order(order, timestamp)`: Places an order in the execution queue.
    -   `process_orders(current_time, price_data)`: Processes orders that are ready to be executed.

### Performance

-   **`Performance`**: Calculates performance metrics.
    -   `compute_metrics()`: Computes a summary of performance metrics.
