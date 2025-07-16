from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class Order:
    def __init__(self, asset: str, quantity: float, order_type: str = "market"):
        self.asset = asset
        self.quantity = quantity
        self.order_type = order_type


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
    def apply(self, price: float) -> float:
        pass


class GaussianSlippage(SlippageModel):
    def __init__(self, mu: float = 0, sigma: float = 0.001):
        self.mu = mu
        self.sigma = sigma

    def apply(self, price: float) -> float:
        return price * (1 + random.gauss(self.mu, self.sigma))


class Execution:
    def __init__(
        self,
        commission_model: CommissionModel = FlatCommission(),
        slippage_model: SlippageModel = GaussianSlippage(),
    ):
        self.commission_model = commission_model
        self.slippage_model = slippage_model

    def process_orders(
        self, orders: List[Order], price_data: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        fills = []
        for order in orders:
            price = price_data[order.asset]
            slipped_price = self.slippage_model.apply(price)
            commission = self.commission_model.calculate(
                order.quantity, slipped_price
            )
            fills.append(
                {
                    "asset": order.asset,
                    "quantity": order.quantity,
                    "price": slipped_price,
                    "commission": commission,
                }
            )
        return fills
