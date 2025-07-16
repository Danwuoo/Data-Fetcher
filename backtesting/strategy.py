from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from backtesting.execution import Order
from backtesting.events import MarketData


class Strategy(ABC):
    @abstractmethod
    def on_data(self, event: MarketData) -> List[Order]:
        pass
