from enum import Enum

from metrics.slice.types import StockSliceMetricsName_Base

PRICE_HISTORY_DATA_FRAME_INDEX_NAME = "Datetime"


class PriceHistoryDataFrameMetricsName_Base(str, Enum):
    OPEN = "Open"
    HIGH = "High"
    LOW = "Low"
    CLOSE = "Close"
    VOLUME = "Volume"


class PriceHistoryDataFrameMetricsName_BaseAdapter:
    _METRICS_MAP = {
        PriceHistoryDataFrameMetricsName_Base.OPEN: StockSliceMetricsName_Base.OPEN.value,
        PriceHistoryDataFrameMetricsName_Base.HIGH: StockSliceMetricsName_Base.HIGH.value,
        PriceHistoryDataFrameMetricsName_Base.LOW: StockSliceMetricsName_Base.LOW.value,
        PriceHistoryDataFrameMetricsName_Base.CLOSE: StockSliceMetricsName_Base.CLOSE.value,
        PriceHistoryDataFrameMetricsName_Base.VOLUME: StockSliceMetricsName_Base.VOLUME.value,
    }

    _INDEX_MAP = {
        PRICE_HISTORY_DATA_FRAME_INDEX_NAME: StockSliceMetricsName_Base.DATETIME.value
    }

    @classmethod
    def conv(cls, metric_name: PriceHistoryDataFrameMetricsName_Base):
        return cls._METRICS_MAP[metric_name]

    @classmethod
    def conv_id(cls, id_name: str):
        return cls._INDEX_MAP[id_name]
