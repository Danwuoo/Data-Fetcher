from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Union

import numpy as np
import polars as pl

from backtest_data_module.backtesting.events import Event, SignalEvent


class StrategyBase(ABC):
    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def on_data(self, event: Union[np.ndarray, pl.DataFrame]) -> List[SignalEvent]:
        pass

    def on_start(self, context):
        pass

    def on_finish(self, context):
        pass
