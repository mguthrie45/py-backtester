"""
Trade slice models and related types (from model.data.trade_slices + model.data.types).
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

from model.reporting import JSONable, ObservableTrade


class TradeActionType(str, Enum):
    BUY = "buy"
    SELL = "sell"


class TradeAction(BaseModel, JSONable, ObservableTrade):
    dt: datetime
    type: TradeActionType
    ticker: str
    num_shares: float = Field(..., gt=0)

    @staticmethod
    def obs_field_prefix() -> str:
        return "a"

    @property
    def json(self) -> dict:
        d = {
            "dt": self.dt,
            "ticker": self.ticker,
            "type": self.type.value,
            "num_shares": self.num_shares,
        }

        return d

    @property
    def obs_json(self) -> dict:
        d = {
            "dt": self.dt,
            "ticker": self.ticker,
            "type": self.type.value,
            "num_shares": self.num_shares,
        }

        return d

    @staticmethod
    def obs_fields() -> list[str]:
        return ["dt", "ticker", "type", "num_shares"]

    @staticmethod
    def from_tuple(action_tuple: tuple, dt: datetime) -> "TradeAction":
        return TradeAction(
            dt=dt,
            type=action_tuple[0],
            ticker=action_tuple[1],
            num_shares=action_tuple[2],
        )


@dataclass
class TradeActionResult:
    dt: datetime
    profit: float
