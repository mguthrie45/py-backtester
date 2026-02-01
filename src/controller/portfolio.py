from datetime import datetime
from functools import reduce

from log.logger import Logger
from model.data.Holding import HoldingType, Holding
from model.data.StockSlice import StockSlice
from typing import Optional


class PortfolioController:
    debt: float = 0
    bought: float = 0
    returns: float = 0
    holdings: dict[str, Holding]

    def __init__(self, value: int):
        self.cash_value = value
        self.holdings = {}

    # TODO: Should we use different num_shares?
    def buy(self, stock_slice: StockSlice, num_shares=1) -> bool:
        Logger.debug(f"buy: \n%s, num_shares=%d", stock_slice, num_shares)

        holding = Holding(
            type=HoldingType.BUY,
            purchased_dt=datetime.now(),
            buy_price=stock_slice.close,
            curr_price=stock_slice.close,
            num_shares=num_shares,
        )
        Logger.debug("holding created: \n%s", holding)

        self.__add_holding(holding)

        return True

    # TODO: num_shares?
    # TODO: holding_id?
    def sell(self, holding_id: Optional[str] = None) -> bool:
        if holding_id:
            holding = self.holdings[holding_id]
            if holding:
                self.__rem_holding(holding_id)
                self.returns += holding.profit
                return True
            else:
                raise Exception(
                    f"Attempted to sell holding '{holding_id}' but it doest not exist."
                )
        else:
            holdings_to_sell = list(
                filter(lambda h: h.type == HoldingType.BUY, self.holdings.values())
            )

            if len(holdings_to_sell) == 0:
                return False

            for holding in holdings_to_sell:
                self.__rem_holding(holding.id)
                self.returns += holding.profit

            return True

    def exec_all_holdings(self) -> None:
        self.sell()

    def update_holdings(self, stock_slice: StockSlice) -> None:
        for holding in self.holdings.values():
            holding.curr_price = stock_slice.close

    def __add_holding(self, holding: Holding):
        match holding.type:
            case HoldingType.BUY:
                self.bought += holding.value
            case HoldingType.SHORT:
                # TODO: wrong
                self.debt += holding.value

        self.holdings[holding.id] = holding

    def __rem_holding(self, holding_id: str):
        del self.holdings[holding_id]

    @property
    def value(self) -> float:
        return (
            self.returns
            + reduce(lambda acc, h: acc + h.value, self.holdings.values(), 0)
            - self.debt
        )
