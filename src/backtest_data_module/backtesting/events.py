from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class MarketData:
    data: Dict[str, Dict[str, float]]
