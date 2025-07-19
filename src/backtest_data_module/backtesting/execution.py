from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from datetime import timedelta
from typing import Dict, List

import numpy as np

from backtest_data_module.backtesting.events import OrderEvent, FillEvent


class CommissionModel(ABC):
    @abstractmethod
    def calculate(self, quantity: float, price: float) -> float:
        pass


class FlatCommission(CommissionModel):
    def __init__(self, fee: float = 0.0005):
        self.fee = fee

    def calculate(self, quantity: float, price: float) -> float:
        return self.fee * abs(quantity) * price


class SlippageModel(ABC):
    @abstractmethod
    def apply(self, price: float, quantity: float) -> float:
        pass


class GaussianSlippage(SlippageModel):
    def __init__(self, mu: float = 0, sigma: float = 0.001, seed: int = None):
        self.mu = mu
        self.sigma = sigma
        self.rng = np.random.default_rng(seed)

    def apply(self, price: float, quantity: float) -> float:
        # Simulate slippage based on a normal distribution
        # A more advanced model could also consider the order size (quantity)
        return price["close"] * (1 + self.rng.normal(self.mu, self.sigma))


class LatencyModel(ABC):
    @abstractmethod
    def get_delay(self) -> float:
        pass


class PoissonLatency(LatencyModel):
    def __init__(self, lam: float = 0.01):  # Average delay of 10ms
        self.lam = lam

    def get_delay(self) -> float:
        return np.random.poisson(self.lam)


class Execution:
    def __init__(
        self,
        commission_model: CommissionModel = FlatCommission(),
        slippage_model: SlippageModel = GaussianSlippage(seed=42),
        latency_model: LatencyModel = PoissonLatency(),
    ):
        self.commission_model = commission_model
        self.slippage_model = slippage_model
        self.latency_model = latency_model
        self.order_queue = deque()

    def place_order(self, order: OrderEvent, timestamp: float):
        delay = self.latency_model.get_delay()
        execution_time = timestamp + timedelta(seconds=delay)
        self.order_queue.append((order, execution_time))

    def process_orders(
        self, current_time: float, price_data: Dict[str, float]
    ) -> List[FillEvent]:
        fills = []

        # 依執行時間排序訂單以維持 FIFO
        self.order_queue = deque(sorted(self.order_queue, key=lambda x: x[1]))

        while self.order_queue and self.order_queue[0][1] <= current_time:
            order, execution_time = self.order_queue.popleft()

            if order.asset not in price_data:
                # 若該資產無價格資料則略過
                # 目前僅直接跳過該訂單
                continue

            price = price_data[order.asset]
            slipped_price = self.slippage_model.apply(price, order.quantity)
            commission = self.commission_model.calculate(order.quantity, slipped_price)

            fills.append(
                {
                    "asset": order.asset,
                    "quantity": order.quantity,
                    "price": slipped_price,
                    "commission": commission,
                }
            )
        return fills
