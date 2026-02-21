from datetime import datetime
from enum import Enum
from typing import Optional

import yaml
from typing_extensions import Annotated

from constants import FILE_PATH_CONFIG_TESTS_DIR
from log.types import LogLevel
from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)


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
        TimeInterval.ONE_MONTH: 30 * 24 * 60 * 60,
        TimeInterval.THREE_MONTH: 3 * 30 * 24 * 60 * 60,
    }

    @classmethod
    def to_seconds(cls, time_interval: TimeInterval) -> int:
        return cls._DELTA_MAP[time_interval]


class DataParams(BaseModel):
    ticker: Annotated[str, StringConstraints(to_upper=True, max_length=4, min_length=4)]
    time_interval: TimeInterval
    batch_size: int = 100
    start_dt: datetime
    end_dt: Optional[datetime] = Field(datetime.now())
    datasource_type: DatasourceType = DatasourceType.YAHOO_FINANCE


class TradingParams(BaseModel):
    evaluation_period: int = Field(..., gt=0)
    init_cap: int = Field(..., gt=0)


class ReportParams(BaseModel):
    gen: bool


class LoggingParams(BaseModel):
    level: Optional[LogLevel] = None


class Test(BaseModel):
    name: str
    strategy: str
    data_params: DataParams
    trading_params: TradingParams
    logging_params: LoggingParams
    report_params: ReportParams

    @model_validator(mode="after")
    def validate_batch_evaluation_alignment(self):
        assert (
            self.data_params.batch_size % self.trading_params.evaluation_period == 0
        ), (
            "data_params.batch_size must be a multiple of trading_params.evaluation_period"
        )
        return self

    @staticmethod
    def from_yaml(name: str):
        path = FILE_PATH_CONFIG_TESTS_DIR / f"{name}.yaml"
        try:
            content = path.read_text(encoding="utf-8")
            try:
                test_yml = yaml.safe_load(content)
                try:
                    return Test.model_validate(test_yml)
                except ValidationError as err:
                    print(f"Test '{name}' is misconfigured.")
                    raise err
            except yaml.YAMLError as err:
                print(err)
                raise err
        except FileNotFoundError as err:
            print(f"Test '{name}' not found.")
            raise err
