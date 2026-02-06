from model.data.Action import Action
from model.data.StockSlice import StockSlice
from model.portfolio.slice import PortfolioSlice
from model.reporting.transaction import Transaction


class Observer:
    def __init__(self):
        self.observations: list[Transaction] = []

    def observe(
        self, action: Action, stock_slice: StockSlice, portfolio_slice: PortfolioSlice
    ) -> None:
        transaction = Transaction(
            action=action, stock_slice=stock_slice, portfolio_slice=portfolio_slice
        )
        self.observations.append(transaction)

    def flush_observations(self) -> None:
        pass
