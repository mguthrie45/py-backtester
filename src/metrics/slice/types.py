from enum import Enum
from typing import Union


class StockSliceMetricsName_Base(str, Enum):
    DATETIME = "dt"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"


class StockSliceMetricsName_Extended(str, Enum):
    RSI_10 = "rsi_10"
    SMA_10 = "sma_10"
    SMA_15 = "sma_15"
    SMA_30 = "sma_30"
    SMA_50 = "sma_50"
    SMA_100 = "sma_100"


class StockSliceExtendedMetricsNameBatchExtensionSize(int, Enum):
    RSI_10 = 10
    SMA_10 = 10
    SMA_15 = 15
    SMA_30 = 30
    SMA_50 = 50
    SMA_100 = 100


class StockSliceExtendedMetricsNameBatchExtensionSizeAdapter:
    _MAP: dict[
        StockSliceMetricsName_Extended, StockSliceExtendedMetricsNameBatchExtensionSize
    ] = {
        StockSliceMetricsName_Extended.RSI_10: StockSliceExtendedMetricsNameBatchExtensionSize.RSI_10,
        StockSliceMetricsName_Extended.SMA_10: StockSliceExtendedMetricsNameBatchExtensionSize.SMA_10,
        StockSliceMetricsName_Extended.SMA_15: StockSliceExtendedMetricsNameBatchExtensionSize.SMA_15,
        StockSliceMetricsName_Extended.SMA_30: StockSliceExtendedMetricsNameBatchExtensionSize.SMA_30,
        StockSliceMetricsName_Extended.SMA_50: StockSliceExtendedMetricsNameBatchExtensionSize.SMA_50,
        StockSliceMetricsName_Extended.SMA_100: StockSliceExtendedMetricsNameBatchExtensionSize.SMA_100,
    }

    @classmethod
    def get_batch_extension_size(
        cls, metric_name: StockSliceMetricsName_Extended
    ) -> StockSliceExtendedMetricsNameBatchExtensionSize:
        return cls._MAP[metric_name]


type StockSliceMetricsName = Union[
    StockSliceMetricsName_Base, StockSliceMetricsName_Extended
]
