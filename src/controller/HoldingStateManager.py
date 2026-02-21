from dataclasses import dataclass

from controller.types import Sliceable
from log.logger import Logger
from model.state_slices import HoldingStateSlice, HoldingStateType


@dataclass
class HoldingState(Sliceable):
    ticker: str
    type: HoldingStateType
    pps: float
    num_shares: float = 0.0

    def __post_init__(self) -> None:
        if self.num_shares < 0:
            raise ValueError("num_shares must be >= 0")

    def update_pps(self, pps: float) -> None:
        self.pps = pps

    def subtract_shares(self, num_shares: float) -> bool:
        if num_shares > self.num_shares:
            Logger.error("Cannot remove more shares than exist.")
            return False

        self.num_shares -= num_shares

        return True

    def add_shares(self, num_shares: float) -> bool:
        self.num_shares += num_shares

        return True

    @property
    def value(self) -> float:
        return self.pps * self.num_shares

    @property
    def slice(self) -> HoldingStateSlice:
        return HoldingStateSlice(
            ticker=self.ticker, type=self.type, num_shares=self.num_shares
        )
