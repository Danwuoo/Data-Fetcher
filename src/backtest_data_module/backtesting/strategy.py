from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Union

import numpy as np
import polars as pl

from backtest_data_module.backtesting.events import Event, SignalEvent


class StrategyBase(ABC):
    def __init__(self, params: dict, device: str = "cpu", precision: str = "fp32", quantization_bits: int | None = None):
        self.params = params
        self.device = device
        self.precision = precision
        self.quantization_bits = quantization_bits

    @abstractmethod
    def on_data(self, event: Union[np.ndarray, pl.DataFrame]) -> List[SignalEvent]:
        pass

    def on_start(self, context):
        pass

    def on_finish(self, context):
        pass
