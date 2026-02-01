from datetime import datetime, timedelta
from typing import Iterator

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
from model.config.TimeInterval import TimeInterval, TimeIntervalAdapter
from strategy.Strategy import Strategy
from model.config.Test import Test
from model.data.StockSlice import StockSlice


class DataPreprocessor:
    __ticker: str
    __strategy: Strategy
    __start_dt: datetime
    __end_dt: datetime | None
    __interval: TimeInterval
    __batch_size: int

    def __init__(self, test: Test, strategy: Strategy):
        Logger.debug("Initializing DataPreprocessor (generator).")
        self.__ticker = test.data_params.ticker
        self.__strategy = strategy
        self.__start_dt = test.data_params.start_dt
        self.__end_dt = test.data_params.end_dt or datetime.now()
        self.__interval = test.data_params.time_interval
        self.__batch_size = test.data_params.batch_size
        self.__max_lookback = self.__strategy.max_lookback
        self.__effective_batch_size = self.__batch_size + self.__max_lookback

    def _load_window(
        self, start: datetime, end: datetime | None, interval: TimeInterval
    ) -> DataFrame:
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
        period_seconds = TimeIntervalAdapter.to_seconds(self.__interval)
        accum_df = pd.DataFrame()
        win_end = start
        win_start = start
        actual_start = None
        actual_end = None

        while accum_df.shape[0] < orig_batch_size:
            batch_delta = timedelta(seconds=batch_size * period_seconds)
            win_start = win_start - batch_delta if is_lookback else win_start
            win_end = win_start if is_lookback else win_start + batch_delta

            if win_end > self.__end_dt:
                Logger.debug(
                    "end_dt reached, final batch may be incomplete. size=%d",
                    accum_df.shape[0],
                )
                return (accum_df, win_end)

            df_batch = self._load_window(win_start, win_end, self.__interval)

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

    def _append_extended_metrics(
        self, df_batch: DataFrame, extended_metrics: ExtendedMetricsProvider
    ) -> DataFrame:
        Logger.debug("Appending extended metrics to DataFrame.")
        for metric_name in self.__strategy.extended_metrics:
            Logger.debug("Appending metric: %s.", metric_name)
            df_batch = pd.merge(
                df_batch,
                getattr(extended_metrics, metric_name),
                how="left",
                on=StockSliceMetricsName_Base.DATETIME.value,
            )
        return df_batch

    def _df_to_slices(
        self, df_batch: DataFrame, last_prev: StockSlice | None
    ) -> list[StockSlice]:
        slices: list[StockSlice] = []
        for dt, row in df_batch.iterrows():
            row_dict = row.to_dict()
            prev = last_prev if len(slices) == 0 else slices[-1]
            stock_slice = StockSlice(dt=dt, prev=prev, **row_dict)
            slices.append(stock_slice)
        if slices:
            Logger.debug(
                "Transformed DataFrame to slices. preview:\n %s",
                "\n".join(map(str, slices[:3])),
            )
        return slices

    def _generate_slice_batches(self) -> Iterator[list[StockSlice]]:
        window_start = self.__start_dt
        last_prev: StockSlice | None = None

        while True:
            df_batch_new, actual_end_dt = self._load_window_exhaustive(
                self.__batch_size, window_start
            )

            df_batch_lookback, _ = self._load_window_exhaustive(
                self.__max_lookback, window_start, True
            )

            df_batch = pd.concat([df_batch_lookback, df_batch_new])

            extended_metrics = ExtendedMetricsProvider(df_batch)
            df_batch = self._append_extended_metrics(df_batch, extended_metrics)
            new_slices = self._df_to_slices(df_batch, last_prev)

            if new_slices:
                last_prev = new_slices[-1]

            yield new_slices

            if actual_end_dt > self.__end_dt:
                break

            window_start = actual_end_dt

    @property
    def slice_batches(self) -> Iterator[list[StockSlice]]:
        """Generator yielding batches of StockSlices (batch_size per batch)."""
        return self._generate_slice_batches()
