from pandas import DataFrame

from metrics.slice.types import (
    StockSliceMetricsName_Base,
    StockSliceMetricsName_Extended,
)


def rsi_10(history: DataFrame) -> DataFrame:
    periods = 10
    delta = history[StockSliceMetricsName_Base.CLOSE].diff(1).dropna()

    loss = delta.copy()
    gains = delta.copy()

    gains[gains < 0] = 0
    loss[loss > 0] = 0

    gain_ewm = gains.ewm(com=periods, min_periods=periods, adjust=False).mean()
    loss_ewm = abs(loss.ewm(com=periods, min_periods=periods, adjust=False).mean())

    rs = gain_ewm / loss_ewm
    rsi = 100 - 100 / (1 + rs)

    return rsi.to_frame(name=StockSliceMetricsName_Extended.RSI_10)
