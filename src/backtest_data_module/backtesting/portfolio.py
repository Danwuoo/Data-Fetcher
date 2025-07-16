from __future__ import annotations

from typing import Dict, List, Any


class Position:
    def __init__(
        self,
        asset: str,
        quantity: float = 0,
        cost_basis: float = 0,
    ):
        self.asset = asset
        self.quantity = quantity
        self.cost_basis = cost_basis
        self.realized_pnl = 0

    def update(self, quantity: float, price: float):
        if self.quantity + quantity == 0:
            self.realized_pnl += (price * quantity) + (self.cost_basis * self.quantity)
            self.cost_basis = 0
        else:
            self.cost_basis = (self.cost_basis * self.quantity + price * quantity) / (
                self.quantity + quantity
            )
        self.quantity += quantity

    def market_value(self, current_price: float) -> float:
        return self.quantity * current_price

    def unrealized_pnl(self, current_price: float) -> float:
        return (current_price - self.cost_basis) * self.quantity


class RiskManager:
    def __init__(self, position_limit: int = 100):
        self.position_limit = position_limit

    def check_risk(self, portfolio: Portfolio, asset: str, quantity: float) -> bool:
        if asset in portfolio.positions:
            current_quantity = portfolio.positions[asset].quantity
            if abs(current_quantity + quantity) > self.position_limit:
                return False
        return True


class Portfolio:
    def __init__(
        self,
        initial_cash: float = 100000.0,
        risk_manager: RiskManager = RiskManager(),
    ):
        self.cash = initial_cash
        self.positions: Dict[str, Position] = {}
        self.fills: List[Dict] = []
        self.risk_manager = risk_manager
        self.context: Dict[str, Any] = {}

    def update(self, fills: List[Dict]):
        for fill in fills:
            asset = fill["asset"]
            quantity = fill["quantity"]
            price = fill["price"]
            commission = fill["commission"]

            if not self.risk_manager.check_risk(self, asset, quantity):
                # For now, we just skip the fill if it violates the risk rules
                continue

            self.cash -= quantity * price + commission
            if asset not in self.positions:
                self.positions[asset] = Position(asset)
            self.positions[asset].update(quantity, price)
            self.fills.append(fill)

    def get_pnl(self, market_data: Dict[str, float]) -> float:
        realized_pnl = sum(
            position.realized_pnl for position in self.positions.values()
        )
        unrealized_pnl = sum(
            position.unrealized_pnl(market_data.get(asset, 0))
            for asset, position in self.positions.items()
        )
        return realized_pnl + unrealized_pnl
