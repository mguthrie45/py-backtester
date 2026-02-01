from pandas import DataFrame

from metrics.slice.types import (
    StockSliceMetricsName_Base,
    StockSliceMetricsName_Extended,
)


def sma_10(data_frame: DataFrame) -> DataFrame:
    sma = data_frame[StockSliceMetricsName_Base.CLOSE].rolling(window=10).mean()
    return sma.to_frame(name=StockSliceMetricsName_Extended.SMA_10.value)
