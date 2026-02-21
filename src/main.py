import sys
from controller.BacktestManager import BacktestManager
from log.logger import Logger
from model.test import Test
from reporting.context import ObservationContext
from reporting.dashboard.downsampler import Downsampler
from reporting.statistics.engine import BacktestReportEngine
from strategy.Strategy import Strategy


# TODO: Plot volume in observations
# TODO: Create ObservationBuffer with write method instead of vanilla list
# TODO: Some accumulators are very memory intensive, how do we "paginatify" some of these metrics?
# TODO: Should we add TradeResults observable to trade_obs??? May help with win rate calculation.
# TODO: Remove datetimes and only deal with iter number
# TODO: CSV dataset datasource implementation
## This implementation should be conscious of the future multi-ticker adaptation
### Different tickers, different files?
# TODO: Adapt stock slice batches and test config to multiple tickers
# TODO: Add benchmark stock that will act as other datasource always
# TODO: Remove expression conditions? Or accept as a less-powerful, easier alternative...
# TODO: Should I move extended metrics to another config that dynamically loads metric functions as modules?
# TODO: Rename/restructure arguments to external conditions as "context".
## Update condition config to adjust which arguments are needed
## The class for this context should be "ConditionContext" and should house the arguments


if __name__ == "__main__":
    test_name = sys.argv[1]
    controller = BacktestManager(test_name)
    controller.backtest()
    controller.generate_report()

    # test = Test.from_yaml(test_name)

    # Logger.__init__(test.logging_params)

    # Logger.debug("Using test: \n%s", test)
    # Logger.info("Using test: %s", test.name)

    # strategy = Strategy.from_yaml(test.strategy)

    # ctx = ObservationContext(
    #     test_name=test.name,
    #     strategy_name=strategy.name,
    #     tickers=[test.data_params.ticker],
    # )

    # report_engine = BacktestReportEngine(ctx)
    # report_engine.run()
