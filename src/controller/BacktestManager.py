from typing import Iterator
from data.datasources import Datasource
from data.processor import DatasourceProcessor
from log.logger import Logger
from reporting.context import ObservationContext
from reporting.dashboard.downsampler import Downsampler
from reporting.observer import Observer
from reporting.statistics.engine import BacktestReportEngine
from strategy.Strategy import Strategy
from model.config.Test import Test
from model.data.state_slices import StockSlice
from controller.PortfolioManager import PortfolioManager


class BacktestManager:
    test: Test
    strategy: Strategy
    portfolio: PortfolioManager
    slice_batches: Iterator[list[StockSlice]]

    iter_delta: int
    curr_slice_idx: int

    def __init__(self, test_name: str):
        self.test = Test.from_yaml(test_name)

        Logger.__init__(self.test.logging_params)

        Logger.debug("Using test: \n%s", self.test)
        Logger.info("Using test: %s", self.test.name)

        self.strategy = Strategy.from_yaml(self.test.strategy)

        self.observation_ctx = ObservationContext(
            test_name=test_name,
            strategy_name=self.strategy.name,
            tickers=[self.test.data_params.ticker],
        )
        Observer(self.observation_ctx)

        self.report_engine = BacktestReportEngine(self.observation_ctx)
        self.downsampler = Downsampler(self.observation_ctx)

        Logger.debug("Using strategy: \n%s", self.strategy)
        Logger.info("Using strategy: %s", self.strategy.name)

        self.portfolio = PortfolioManager(self.test.trading_params.init_cap)
        Logger.debug("Portfolio initialized.")

        yf_datasource = Datasource(self.test, self.strategy)
        Logger.debug("Yahoo finance datasource initialized.")

        processor = DatasourceProcessor(yf_datasource, self.test, self.strategy)
        Logger.debug("Datasource processor initialized.")

        self.slice_batches = processor.slice_batches

        self.curr_slice_idx = 0

    def generate_report(self):
        self.report_engine.run()
        self.downsampler.run()

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
                Observer.get_instance().observe(self.portfolio.capital_state.slice)
                for holding_state in self.portfolio.holdings.holdings.values():
                    Observer.get_instance().observe(holding_state.slice)

                slice_idx = (
                    i * self.test.trading_params.evaluation_period
                    + self.strategy.max_lookback
                )
                stock_slice = slice_batch[slice_idx]
                Observer.get_instance().observe(stock_slice)

                Logger.debug("Evaluating slice: \n%s", stock_slice)

                slices_with_lookback = slice_batch[
                    slice_idx - self.strategy.max_lookback : slice_idx + 1
                ]

                if len(slices_with_lookback) != self.strategy.max_lookback + 1:
                    raise Exception(
                        "Invalid slice window. Expected %d slices, received %d",
                        self.strategy.max_lookback + 1,
                        len(slices_with_lookback),
                    )

                self.portfolio.holdings.update_pps(
                    {self.test.data_params.ticker: stock_slice.close}
                )

                actions = self.strategy.eval(
                    slices_with_lookback,
                    [self.portfolio.capital_state.slice],
                    self.portfolio.holdings.slices,
                )
                Logger.debug("TradeActions evaluated to: %s", actions)

                for action in actions:
                    if self.portfolio.exec_action(action, ask=stock_slice.close):
                        Logger.info(
                            "TradeAction '%s' executed for slice: \n%s",
                            action.type.value,
                            stock_slice,
                        )

                        Observer.get_instance().observe(action)
                        break

                Observer.get_instance().pack_observation_pool()

        Observer.get_instance().shutdown()
