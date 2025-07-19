from __future__ import annotations

import importlib
import pkgutil
from typing import Dict, Type

from backtest_data_module.backtesting.strategy import StrategyBase


class StrategyRegistry:
    """A registry for strategies.

    This class is responsible for discovering, registering, and loading strategies.
    Strategies can be registered manually or discovered automatically from a given path.
    """

    def __init__(self) -> None:
        self._strategies: Dict[str, Type[StrategyBase]] = {}

    def register(self, name: str, strategy_cls: Type[StrategyBase]) -> None:
        """Register a strategy.

        Args:
            name: The name of the strategy.
            strategy_cls: The strategy class.
        """
        if name in self._strategies:
            raise ValueError(f"Strategy '{name}' is already registered.")
        self._strategies[name] = strategy_cls

    def discover(self, path: str) -> None:
        """Discover strategies from a given path.

        This method will search for modules in the given path and try to
        import them. It will then look for subclasses of `StrategyBase`
        in the imported modules and register them automatically.

        Args:
            path: The path to search for strategies.
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
        """Get a strategy by name.

        Args:
            name: The name of the strategy.

        Returns:
            The strategy class.
        """
        if name not in self._strategies:
            raise ValueError(f"Strategy '{name}' is not registered.")
        return self._strategies[name]


strategy_registry = StrategyRegistry()
