from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator

from pandas import DataFrame

from log.logger import Logger
from model.config.TimeInterval import TimeInterval
from model.config.Test import Test
from strategy.Strategy import Strategy


class AbstractDatasource(ABC, object):
    _stategy: Strategy
    _test: Test

    def __init__(self, test: Test, strategy: Strategy):
        Logger.debug("Initializing Datasource.")
        self._test = test
        self._stategy = strategy

    @abstractmethod
    def _load_window(
        self, start: datetime, end: datetime | None, interval: TimeInterval
    ) -> DataFrame:
        pass

    @property
    @abstractmethod
    def df_batches(self) -> Iterator[DataFrame]:
        pass
