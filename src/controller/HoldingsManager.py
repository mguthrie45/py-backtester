from collections import defaultdict
from controller.HoldingStateManager import HoldingState
from log.logger import Logger
from model.state_slices import HoldingStateSlice, HoldingStateType
from model.trade_slices import TradeAction, TradeActionType


class HoldingsManager:
    holdings: dict[str, HoldingState]

    def __init__(self):
        self.holdings = {}

    def update_pps(self, pps_dict: dict[str, float]) -> None:
        for ticker, pps in pps_dict.items():
            holding = self.holdings.get(ticker)
            if holding:
                holding.update_pps(pps)

    def update_holding(self, action: TradeAction, ask: float) -> bool:
        match action.type:
            case TradeActionType.BUY:
                return self.__update_holding_buy(
                    ticker=action.ticker,
                    num_shares=action.num_shares,
                    ask=ask,
                )
            case TradeActionType.SELL:
                return self.__update_holding_sell(
                    ticker=action.ticker, num_shares=action.num_shares
                )
            case _:
                raise Exception(f"TradeAction '{action}' is unknown.")

    def __update_holding_buy(self, ticker: str, num_shares: float, ask: float) -> bool:
        if ticker in self.holdings:
            holding = self.holdings[ticker]
            holding.add_shares(num_shares)
        else:
            holding = HoldingState(
                ticker=ticker, type=HoldingStateType.BUY, pps=ask, num_shares=num_shares
            )
            self.__add_holding(holding)
            Logger.debug("holding created: \n%s", holding)

        return True

    def __update_holding_sell(self, ticker: str, num_shares: float) -> bool:
        holding = self.holdings[ticker]
        if not holding.subtract_shares(num_shares):
            return False

        if holding.num_shares <= 0:
            self.__rem_holding(ticker)

        return True

    def __add_holding(self, holding: HoldingState):
        self.holdings[holding.ticker] = holding

    def rem_holding(self, ticker: str):
        del self.holdings[ticker]

    @property
    def value(self) -> float:
        return sum(map[float](lambda h: h.value, self.holdings.values()))

    @property
    def slices(self) -> defaultdict[str, list[HoldingStateSlice]]:
        d = defaultdict[str, list](list)
        for ticker, state in self.holdings.items():
            d[ticker].append(state.slice)

        return d
