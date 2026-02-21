"""
Observation types and reporting metadata (combined from model.reporting.observable and types).
"""
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path
from typing import Optional


class JSONable(ABC):
    @property
    def json(self) -> dict:
        pass


class Observable(ABC):
    ticker: Optional[str] = None

    @staticmethod
    @abstractmethod
    def obs_field_prefix() -> str:
        """A prefix to namespace the attributes"""

    @staticmethod
    @abstractmethod
    def obs_fields() -> list[str]:
        """The attrs to observe"""

    @property
    @abstractmethod
    def obs_json(self) -> dict:
        """JSON representation of observation"""


class ObservableState(Observable):
    ticker: Optional[str] = None


class ObservableTrade(Observable):
    ticker: str


class ObservationType(Enum):
    STATE = ObservableState
    TRADE = ObservableTrade


@dataclass
class ObservationTypeMetadata(JSONable):
    num_records: int = 0
    tickers: list[str] = field(default_factory=list)
    num_tickers: int = 0

    @property
    def json(self) -> dict:
        return self.__dict__

    @staticmethod
    def from_json(path: str | Path) -> "ObservationTypeMetadata":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return ObservationTypeMetadata(**data)


@dataclass
class ObservationTypeState(JSONable):
    obs_pool: defaultdict[str, list[Observable]] = field(
        default_factory=lambda: defaultdict(list)
    )
    obs_slices: list[dict] = field(default_factory=list)
    metadata: ObservationTypeMetadata = field(default_factory=ObservationTypeMetadata)
