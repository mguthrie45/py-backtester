from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, Field

from model.data.types import TradeActionType, JSONable
from model.reporting.observable import ObservableTrade


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
