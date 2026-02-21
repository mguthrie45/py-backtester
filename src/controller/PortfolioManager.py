from controller.CapitalStateManager import CapitalState
from controller.HoldingsManager import HoldingsManager
from log.logger import Logger
from model.state_slices import DataFrameable
from model.trade_slices import TradeAction


class PortfolioManager(DataFrameable):
    capital_state: CapitalState
    holdings: HoldingsManager

    def __init__(self, init_cap: float):
        self.capital_state = CapitalState(cash=init_cap)
        self.holdings = HoldingsManager()

    def exec_action(self, action: TradeAction, ask: float) -> bool:
        if not self.capital_state.update_capital(action=action, ask=ask):
            Logger.error("Unable to modify capital state, skipping action.")
            return False

        return self.holdings.update_holding(action=action, ask=ask)

    @property
    def value(self) -> float:
        return self.capital_state.value + self.holdings.value
