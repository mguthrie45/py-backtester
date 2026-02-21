from enum import Enum


class DatasourceType(str, Enum):
    YAHOO_FINANCE = "yfinance"
    CSV_DATASET = "csv"


class TimeInterval(str, Enum):
    ONE_MINUTE = "1m"
    TWO_MINUTE = "2m"
    FIVE_MINUTE = "5m"
    FIFTEEN_MINUTE = "15m"
    THIRTY_MINUTE = "30m"
    SIXTY_MINUTE = "60m"
    NINETY_MINUTE = "90m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"
    FIVE_DAY = "5d"
    ONE_WEEK = "1wk"
    ONE_MONTH = "1mo"
    THREE_MONTH = "3mo"


class TimeIntervalAdapter:
    _DELTA_MAP: dict[TimeInterval, int] = {
        TimeInterval.ONE_MINUTE: 60,
        TimeInterval.TWO_MINUTE: 2 * 60,
        TimeInterval.FIVE_MINUTE: 5 * 60,
        TimeInterval.FIFTEEN_MINUTE: 15 * 60,
        TimeInterval.THIRTY_MINUTE: 30 * 60,
        TimeInterval.SIXTY_MINUTE: 60 * 60,
        TimeInterval.NINETY_MINUTE: 90 * 60,
        TimeInterval.ONE_HOUR: 60 * 60,
        TimeInterval.ONE_DAY: 24 * 60 * 60,
        TimeInterval.FIVE_DAY: 5 * 24 * 60 * 60,
        TimeInterval.ONE_WEEK: 7 * 24 * 60 * 60,
        TimeInterval.ONE_MONTH: 30 * 24 * 60 * 60,  # Approximation: 30 days
        TimeInterval.THREE_MONTH: 3 * 30 * 24 * 60 * 60,  # Approximation: 90 days
    }

    @classmethod
    def to_seconds(cls, time_interval: TimeInterval) -> int:
        return cls._DELTA_MAP[time_interval]
