from typing import Iterator

import pandas as pd
from pandas import DataFrame
from datasource.datasource_singleton import Datasource
from log.logger import Logger
from metrics.slice.extended.provider import ExtendedMetricsProvider
from metrics.slice.types import StockSliceMetricsName_Base
from model.data.StockSlice import StockSlice
from strategy.Strategy import Strategy


class DatasourceProcessor:
    __datasource: Datasource
    __strategy: Strategy

    def __init__(self, datasource: Datasource, strategy: Strategy):
        self.__datasource = datasource
        self.__strategy = strategy

    def _augment_extended_metrics(
        self, df_batch: DataFrame, extended_metrics: ExtendedMetricsProvider
    ) -> DataFrame:
        Logger.debug("Augmenting extended metrics to DataFrame.")
        for metric_name in self.__strategy.extended_metrics:
            Logger.debug("Augmented metric: %s.", metric_name)
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

    @property
    def slice_batches(self) -> Iterator[list[StockSlice]]:
        last_prev: StockSlice | None = None

        for df_batch in self.__datasource.instance.df_batches:
            extended_metrics = ExtendedMetricsProvider(df_batch)
            extended_df_batch = self._augment_extended_metrics(
                df_batch, extended_metrics
            )
            new_slices = self._df_to_slices(extended_df_batch, last_prev)

            if new_slices:
                last_prev = new_slices[-1]

            yield new_slices
