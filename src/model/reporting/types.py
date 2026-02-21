from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from model.data.types import JSONable
from model.reporting.observable import Observable, ObservableState, ObservableTrade


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
