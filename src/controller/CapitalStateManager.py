from dataclasses import dataclass

from controller.types import Sliceable
from log.logger import Logger
from model.data.trade_slices import TradeAction, TradeActionType
from model.data.state_slices import CapitalStateSlice


@dataclass
class CapitalState(Sliceable):
    cash: float
    debt: float = 0.0

    def update_capital(self, action: TradeAction, ask: float) -> bool:
        match action.type:
            case TradeActionType.BUY:
                return self.rem_cash(val=ask * action.num_shares)
            case TradeActionType.SELL:
                return self.add_cash(val=ask * action.num_shares)
            case _:
                raise Exception(f"TradeAction '{action}' is unknown.")

    def add_cash(self, val: float) -> bool:
        self.cash += val
        return True

    def rem_cash(self, val: float) -> bool:
        if val > self.cash:
            Logger.error("Cannot remove more cash than available.")
            return False

        self.cash -= val

        return True

    def add_debt(self, val: float) -> bool:
        self.debt += val

    def rem_debt(self, val: float) -> bool:
        if val > self.debt:
            Logger.error("Cannot remove more debt than available.")
            return False

        self.debt -= val

        return True

    @property
    def value(self) -> float:
        return self.cash - self.debt

    @property
    def slice(self) -> CapitalStateSlice:
        return CapitalStateSlice(cash=self.cash, debt=self.debt)
