from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Literal

EventType = Literal["MARKET", "SIGNAL", "ORDER", "FILL"]


@dataclass
class Event:
    type: EventType


@dataclass
class MarketEvent(Event):
    type: EventType = "MARKET"
    data: Dict[str, Dict[str, float]] | None = None


@dataclass
class SignalEvent(Event):
    type: EventType = "SIGNAL"
    asset: str | None = None
    quantity: float | None = None
    direction: str | None = None


@dataclass
class OrderEvent(Event):
    type: EventType = "ORDER"
    asset: str | None = None
    quantity: float | None = None
    order_type: str = "market"


@dataclass
class FillEvent(Event):
    type: EventType = "FILL"
    asset: str | None = None
    quantity: float | None = None
    price: float | None = None
    commission: float | None = None
    exchange: str | None = None
