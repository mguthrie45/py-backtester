"""
State slice models and related types (from model.data.state_slices + model.data.types).
"""
from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Optional

from pandas import DataFrame
from pydantic import BaseModel, Field

from model.reporting import JSONable, ObservableState


class HoldingStateType(str, Enum):
    BUY = "buy"
    SHORT = "short"


class DataFrameable(ABC):
    @property
    def df(self) -> DataFrame:
        pass


class StockSlice(BaseModel, JSONable, ObservableState):
    dt: datetime
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int

    rsi_10: Optional[float] = None
    sma_10: Optional[float] = None
    sma_50: Optional[float] = None

    @staticmethod
    def obs_field_prefix() -> str:
        return "s"

    @property
    def json(self) -> dict:
        d = {
            "dt": self.dt,
            "ticker": self.ticker,
            "close": self.close,
            "volume": self.volume,
        }

        if self.rsi_10:
            d["rsi_10"] = self.rsi_10
        if self.sma_10:
            d["sma_10"] = self.sma_10
        if self.rsi_10:
            d["sma_50"] = self.sma_50

        return d

    @property
    def obs_json(self) -> dict:
        d = {
            "dt": self.dt,
            "ticker": self.ticker,
            "close": self.close,
            "volume": self.volume,
        }

        return d

    @staticmethod
    def obs_fields() -> list[str]:
        return ["dt", "ticker", "close", "volume"]

    def __str__(self) -> str:
        return f"dt: {self.dt.isoformat()}  close: {self.close}"


class StockSliceWindow(BaseModel, DataFrameable):
    slices: list[StockSlice]

    @property
    def df(self) -> DataFrame:
        slices_json = list(map(lambda s: s.json, self.slices))
        return DataFrame.from_records(slices_json)


class HoldingStateSlice(BaseModel, JSONable, ObservableState):
    ticker: str
    type: HoldingStateType
    num_shares: float = Field(..., gte=0)

    @staticmethod
    def obs_field_prefix() -> str:
        return "h"

    @property
    def json(self) -> dict:
        return {"type": self.type.value, "num_shares": self.num_shares}

    @property
    def obs_json(self) -> dict:
        d = {
            "ticker": self.ticker,
            "type": self.type.value,
            "num_shares": self.num_shares,
        }

        return d

    @staticmethod
    def obs_fields() -> list[str]:
        return ["ticker", "type", "num_shares"]


class HoldingStateSliceWindow(BaseModel, DataFrameable):
    slices: list[HoldingStateSlice]

    @property
    def df(self) -> DataFrame:
        slices_json = list(map(lambda h: h.json, self.slices))
        return DataFrame.from_records(slices_json)


class CapitalStateSlice(BaseModel, JSONable, ObservableState):
    cash: float = Field(..., gte=0)
    debt: float = Field(..., gte=0)

    @staticmethod
    def obs_field_prefix() -> str:
        return "cap"

    @property
    def json(self) -> dict:
        return {
            "cash": self.cash,
            "debt": self.debt,
        }

    @property
    def obs_json(self) -> dict:
        return {
            "cash": self.cash,
            "debt": self.debt,
        }

    @staticmethod
    def obs_fields() -> list[str]:
        return ["cash", "debt"]


class CapitalStateSliceWindow(BaseModel, DataFrameable):
    slices: list[CapitalStateSlice]

    @property
    def df(self) -> DataFrame:
        slices_json = list(map(lambda s: s.json, self.slices))
        return DataFrame.from_records(slices_json)
