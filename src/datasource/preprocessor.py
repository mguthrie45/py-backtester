from datetime import datetime

import pandas as pd
from pandas import DataFrame
import yfinance

from datasource.types import (
    PRICE_HISTORY_DATA_FRAME_INDEX_NAME,
    PriceHistoryDataFrameMetricsName_Base,
    PriceHistoryDataFrameMetricsName_BaseAdapter,
)
from log.logger import Logger
from metrics.slice.extended.provider import ExtendedMetricsProvider
from metrics.slice.types import StockSliceMetricsName_Base
from model.config.TimeInterval import TimeInterval
from strategy.Strategy import Strategy
from model.config.Test import Test
from model.data.StockSlice import StockSlice


class DataPreprocessor:
    _history: DataFrame
    _slices: list[StockSlice]
    __ticker: str
    __strategy: Strategy

    def __init__(self, test: Test, strategy: Strategy):
        Logger.debug("Initializing DataPreprocessor.")
        self.__ticker = test.data_params.ticker
        self.__strategy = strategy

        self.load(
            start=test.data_params.start_dt,
            end=test.data_params.end_dt,
            interval=test.data_params.time_interval,
        )

        self.extended_metrics = ExtendedMetricsProvider(self._history)
        self.append_extended_metrics()

        self.transform_to_stock_slices()

    def load(self, start: datetime, end: datetime, interval: TimeInterval):
        Logger.debug(
            "Fetching series for: ticker=%s start_dt=%s end_dt=%s int=%s",
            self.__ticker,
            start,
            end,
            interval,
        )

        history = yfinance.Ticker(self.__ticker).history(
            start=start, end=end, interval=interval.value
        )
        Logger.debug("Loaded series, count=%d", history.size)

        Logger.debug("Transforming base metric names.")
        rename_dict = {
            old.value: PriceHistoryDataFrameMetricsName_BaseAdapter.conv(old)
            for old in PriceHistoryDataFrameMetricsName_Base
        }

        self._history = history.rename(columns=rename_dict).rename_axis(
            PriceHistoryDataFrameMetricsName_BaseAdapter.conv_id(
                PRICE_HISTORY_DATA_FRAME_INDEX_NAME
            )
        )

        Logger.debug("Loaded series. preview:\n %s", self._history)

    def append_extended_metrics(self):
        Logger.debug("Appending extended metrics to DataFrame.")

        for metric_name in self.__strategy.extended_metrics:
            Logger.debug("Appending metric: %s.", metric_name)

            self._history = pd.merge(
                self._history,
                getattr(self.extended_metrics, metric_name),
                how="left",
                on=StockSliceMetricsName_Base.DATETIME.value,
            )

    def transform_to_stock_slices(self):
        Logger.debug("Transform to stock slices.")

        self._slices = []

        for dt, row in self._history.iterrows():
            row_dict = row.to_dict()
            prev = None if len(self._slices) == 0 else self._slices[-1]

            stock_slice = StockSlice(dt=dt, prev=prev, **row_dict)

            self._slices.append(stock_slice)

        Logger.debug(
            "Transformed DataFrame to slices. preview:\n %s",
            "\n".join(map(lambda s: str(s), self._slices[:3])),
        )

    @property
    def slices(self) -> list[StockSlice]:
        return self._slices
