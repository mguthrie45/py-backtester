from abc import ABC
from enum import Enum

from pandas import DataFrame


class HoldingStateType(str, Enum):
    BUY = "buy"
    SHORT = "short"


class TradeActionType(str, Enum):
    BUY = "buy"
    SELL = "sell"


class JSONable(ABC):
    @property
    def json(self) -> dict:
        pass


class DataFrameable(ABC):
    @property
    def df(self) -> DataFrame:
        pass
