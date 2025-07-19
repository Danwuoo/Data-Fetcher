# Plugin 架構

本回測框架採用外掛式架構載入自訂策略，藉此將核心框架與使用者策略解耦，方便擴充與維護。

## Strategy Registry

外掛系統的核心元件為 `StrategyRegistry`，位於 `src/backtest_data_module/strategy_manager/registry.py`。此類別負責探索、註冊並載入策略。

### 註冊方式

策略可透過兩種方式註冊：

1. **手動註冊**：直接使用 `strategy_registry` 物件的 `register` 方法註冊策略。

    ```python
    from backtest_data_module.strategy_manager.registry import strategy_registry
    from my_strategies import MyAwesomeStrategy

    strategy_registry.register("MyAwesomeStrategy", MyAwesomeStrategy)
    ```

2. **自動探索**：`discover` 方法可從指定目錄自動尋找並註冊策略。該方法會搜尋模組、匯入後，將繼承自 `StrategyBase` 的類別自動註冊。

    ```python
    from backtest_data_module.strategy_manager.registry import strategy_registry

    strategy_registry.discover("path/to/my/strategies")
    ```

### 使用方式

策略註冊後，即可在建立 `Orchestrator` 時以策略名稱指定：

```python
from backtest_data_module.backtesting.orchestrator import Orchestrator

orchestrator = Orchestrator(
    data_handler=data_handler,
    strategy_name="MyAwesomeStrategy",
    # ... 其他參數
)
```

## 核心模組 API

以下簡要說明回測框架各核心模組的 API。

### Strategy

- **`StrategyBase`**：所有策略的抽象基底類別。
    - `on_data(event)`：必須實作，收到新資料事件時被呼叫，應回傳 `SignalEvent` 列表。
    - `on_start(context)`：回測開始時呼叫。
    - `on_finish(context)`：回測結束時呼叫。

### DataHandler

- **`DataHandler`**：集中管理資料存取的介面。
    - `read(query, tiers, compressed_cols)`：從指定層級讀取資料。
    - `stream(symbols, freq)`：非同步串流資料。

### Execution

- **`Execution`**：處理下單與成交。
    - `place_order(order, timestamp)`：將訂單放入佇列。
    - `process_orders(current_time, price_data)`：處理已到執行時間的訂單。

### Performance

- **`Performance`**：計算績效指標。
    - `compute_metrics()`：回傳績效摘要。
