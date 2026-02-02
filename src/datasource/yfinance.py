from datetime import datetime, timedelta
from typing import Iterator

import pandas as pd
from pandas import DataFrame
import yfinance

from datasource.abstract_datasource import AbstractDatasource
from datasource.types import (
    PRICE_HISTORY_DATA_FRAME_INDEX_NAME,
    PriceHistoryDataFrameMetricsName_Base,
    PriceHistoryDataFrameMetricsName_BaseAdapter,
)
from log.logger import Logger
from model.config.TimeInterval import TimeInterval, TimeIntervalAdapter
from strategy.Strategy import Strategy
from model.config.Test import Test


class YFinanceDatasource(AbstractDatasource):
    def __init__(self, test: Test, strategy: Strategy):
        super(YFinanceDatasource, self).__init__(test, strategy)

    def _load_window(
        self, start: datetime, end: datetime | None, interval: TimeInterval
    ) -> DataFrame:
        Logger.debug(
            "Fetching series for: ticker=%s start_dt=%s end_dt=%s int=%s",
            self._test.data_params.ticker,
            start,
            end,
            interval,
        )
        history = yfinance.Ticker(self._test.data_params.ticker).history(
            start=start, end=end, interval=interval.value
        )
        Logger.debug("Loaded series, count=%d", history.shape[0])

        if history.empty:
            return history

        Logger.debug("Transforming base metric names.")
        rename_dict = {
            old.value: PriceHistoryDataFrameMetricsName_BaseAdapter.conv(old)
            for old in PriceHistoryDataFrameMetricsName_Base
        }
        df_batch = history.rename(columns=rename_dict).rename_axis(
            PriceHistoryDataFrameMetricsName_BaseAdapter.conv_id(
                PRICE_HISTORY_DATA_FRAME_INDEX_NAME
            )
        )
        Logger.debug("Loaded series. preview:\n %s", df_batch)
        return df_batch

    def _load_window_exhaustive(
        self, batch_size: int, start: datetime, is_lookback: bool = False
    ) -> DataFrame:
        Logger.debug(
            "Loading batch exhaustively: size=%d start=%s is_lookback=%d",
            batch_size,
            start,
            is_lookback,
        )

        if batch_size == 0:
            return (pd.DataFrame(), start)

        orig_batch_size = batch_size
        period_seconds = TimeIntervalAdapter.to_seconds(
            self._test.data_params.time_interval
        )
        accum_df = pd.DataFrame()
        win_end = start
        win_start = start
        actual_start = None
        actual_end = None

        while accum_df.shape[0] < orig_batch_size:
            batch_delta = timedelta(seconds=batch_size * period_seconds)
            win_start = win_start - batch_delta if is_lookback else win_start
            win_end = win_start if is_lookback else win_start + batch_delta

            if win_end > self._test.data_params.end_dt:
                Logger.debug(
                    "end_dt reached, final batch may be incomplete. size=%d",
                    accum_df.shape[0],
                )
                return (accum_df, win_end)

            df_batch = self._load_window(
                win_start, win_end, self._test.data_params.time_interval
            )

            accum_df = pd.concat([accum_df, df_batch])
            rem_batch_size = orig_batch_size - accum_df.shape[0]
            batch_size = rem_batch_size

            if rem_batch_size == 0:
                actual_start = win_start
                actual_end = win_end

            win_start = win_start if is_lookback else win_end
            win_end = win_start if is_lookback else win_end

        Logger.debug(
            "Batch fulfilled. size=%d scanned_range=%s",
            accum_df.shape[0],
            [str(actual_start), str(start)]
            if is_lookback
            else [str(start), str(actual_end)],
        )

        return (accum_df, win_end)

    @property
    def df_batches(self) -> Iterator[DataFrame]:
        window_start = self._test.data_params.start_dt

        while True:
            df_batch_new, actual_end_dt = self._load_window_exhaustive(
                self._test.data_params.batch_size, window_start
            )

            df_batch_lookback, _ = self._load_window_exhaustive(
                self._stategy.max_lookback, window_start, True
            )

            df_batch = pd.concat([df_batch_lookback, df_batch_new])

            yield df_batch

            if actual_end_dt > self._test.data_params.end_dt:
                break

            window_start = actual_end_dt
