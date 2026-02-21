from pandas import DataFrame

from metrics.slice.extended.rsi_10 import rsi_10

from metrics.slice.extended.sma_10 import sma_10
from metrics.slice.extended.sma_15 import sma_15
from metrics.slice.extended.sma_30 import sma_30
from metrics.slice.extended.sma_50 import sma_50
from metrics.slice.extended.sma_100 import sma_100


class ExtendedMetricsProvider:
    _df_batch: DataFrame

    def __init__(self, history: DataFrame):
        self._df_batch = history

    @property
    def rsi_10(self) -> DataFrame:
        return rsi_10(self._df_batch)

    @property
    def rsi_15(self) -> DataFrame:
        return rsi_10(self._df_batch)

    @property
    def rsi_30(self) -> DataFrame:
        return rsi_10(self._df_batch)

    @property
    def rsi_50(self) -> DataFrame:
        return rsi_10(self._df_batch)

    @property
    def rsi_100(self) -> DataFrame:
        return rsi_10(self._df_batch)

    @property
    def sma_10(self) -> DataFrame:
        return sma_10(self._df_batch)

    @property
    def sma_15(self) -> DataFrame:
        return sma_15(self._df_batch)

    @property
    def sma_30(self) -> DataFrame:
        return sma_30(self._df_batch)

    @property
    def sma_50(self) -> DataFrame:
        return sma_50(self._df_batch)

    @property
    def sma_100(self) -> DataFrame:
        return sma_100(self._df_batch)
