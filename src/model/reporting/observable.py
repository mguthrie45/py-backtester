from abc import ABC, abstractmethod
from typing import Optional


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
