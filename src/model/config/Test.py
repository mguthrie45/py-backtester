from datetime import datetime
from typing import Optional

import yaml
from typing_extensions import Annotated

from constants import FILE_PATH_CONFIG_TESTS_DIR
from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)

from log.types import LogLevel
from model.config.types import DatasourceType, TimeInterval


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
