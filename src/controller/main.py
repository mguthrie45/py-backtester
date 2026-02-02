from typing import Iterator
from datasource.datasource_singleton import Datasource
from datasource.processor import DatasourceProcessor
from log.logger import Logger
from strategy.Strategy import Strategy
from model.config.Test import Test
from model.data.StockSlice import StockSlice
from model.data.Action import Action
from controller.portfolio import PortfolioController


class MainController:
    test: Test
    strategy: Strategy
    portfolio: PortfolioController
    slice_batches: Iterator[list[StockSlice]]

    iter_delta: int
    curr_slice_idx: int

    def __init__(self, test_name: str):
        self.test = Test.from_yaml(test_name)

        Logger.__init__(self.test.logging_params)

        Logger.debug("Using test: \n%s", self.test)
        Logger.info("Using test: %s", self.test.name)

        self.strategy = Strategy.from_yaml(self.test.strategy)

        Logger.debug("Using strategy: \n%s", self.strategy)
        Logger.info("Using strategy: %s", self.strategy.name)

        self.portfolio = PortfolioController(self.test.trading_params.pcpl_usd)
        Logger.debug("Portfolio initialized.")

        yf_datasource = Datasource(self.test, self.strategy)
        Logger.debug("Yahoo finance datasource initialized.")

        processor = DatasourceProcessor(yf_datasource, self.strategy)
        Logger.debug("Datasource processor initialized.")

        self.slice_batches = processor.slice_batches

        self.curr_slice_idx = 0

    def backtest(self):
        Logger.debug("Starting backtest.")

        for slice_batch in self.slice_batches:
            Logger.debug("Processing new slice batch")

            exp_batch_size = (
                self.test.data_params.batch_size + self.strategy.max_lookback
            )
            recv_batch_size = len(slice_batch)
            if recv_batch_size < exp_batch_size:
                Logger.warn(
                    "A slice batch returned is incomplete, some slices may not be evaluated. Expected %d slices, received %d.",
                    exp_batch_size,
                    recv_batch_size,
                )

            num_iter = len(slice_batch) // self.test.trading_params.evaluation_period

            for i in range(num_iter):
                slice_idx = (
                    i * self.test.trading_params.evaluation_period
                    + self.strategy.max_lookback
                )
                print(len(slice_batch), slice_idx, i)
                stock_slice = slice_batch[slice_idx]

                Logger.debug("Evaluating slice: \n%s", stock_slice)

                self.portfolio.update_holdings(stock_slice)

                slices_with_lookback = slice_batch[
                    slice_idx - self.strategy.max_lookback : slice_idx + 1
                ]

                if len(slices_with_lookback) != self.strategy.max_lookback + 1:
                    raise Exception(
                        "Invalid slice window. Expected %d slices, received %d",
                        self.strategy.max_lookback + 1,
                        len(slices_with_lookback),
                    )

                actions = self.strategy.eval(slices_with_lookback, self.portfolio)
                Logger.debug("Actions evaluated to: %s", actions)

                for action in actions:
                    if self.exec_action(stock_slice, action):
                        Logger.info(
                            "Action '%s' executed for slice: \n%s",
                            action.value,
                            stock_slice,
                        )
                        break

        self.portfolio.exec_all_holdings()

    def exec_action(self, stock_slice: StockSlice, action: Action) -> bool:
        match action:
            case Action.BUY:
                return self.portfolio.buy(stock_slice)
            case Action.SELL:
                return self.portfolio.sell()
            case _:
                raise Exception(f"Action '{action}' is unknown.")
