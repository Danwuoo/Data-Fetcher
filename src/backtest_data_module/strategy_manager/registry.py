from __future__ import annotations

import importlib
import pkgutil
from typing import Dict, Type

from backtest_data_module.backtesting.strategy import StrategyBase


class StrategyRegistry:
    """策略註冊表。

    此類別負責探索、註冊並載入策略。
    策略可手動註冊，或自指定路徑自動發現。
    """

    def __init__(self) -> None:
        self._strategies: Dict[str, Type[StrategyBase]] = {}

    def register(self, name: str, strategy_cls: Type[StrategyBase]) -> None:
        """註冊策略。

        Args:
            name: 策略名稱。
            strategy_cls: 策略類別。
        """
        if name in self._strategies:
            raise ValueError(f"Strategy '{name}' is already registered.")
        self._strategies[name] = strategy_cls

    def discover(self, path: str) -> None:
        """從指定路徑自動發現策略。

        此方法會搜尋目錄中的模組並嘗試匯入，
        接著尋找繼承自 `StrategyBase` 的類別並自動註冊。

        Args:
            path: 要搜尋策略的路徑。
        """
        for _, name, _ in pkgutil.iter_modules([path]):
            module = importlib.import_module(f"{path}.{name}")
            for item_name in dir(module):
                item = getattr(module, item_name)
                if (
                    isinstance(item, type)
                    and issubclass(item, StrategyBase)
                    and item is not StrategyBase
                ):
                    self.register(item.__name__, item)

    def get_strategy(self, name: str) -> Type[StrategyBase]:
        """依名稱取得策略。

        Args:
            name: 策略名稱。

        Returns:
            對應的策略類別。
        """
        if name not in self._strategies:
            raise ValueError(f"Strategy '{name}' is not registered.")
        return self._strategies[name]


strategy_registry = StrategyRegistry()
