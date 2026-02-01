from datetime import datetime
from typing import Optional
import yaml
import os

from typing_extensions import Annotated
from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)

from log.types import LoggingParams
from model.config.TimeInterval import TimeInterval


class DataParams(BaseModel):
    ticker: Annotated[str, StringConstraints(to_upper=True, max_length=4, min_length=4)]
    time_interval: TimeInterval
    batch_size: int = 100
    start_dt: datetime
    end_dt: Optional[datetime] = Field(datetime.now())


# TODO: parameter to ignore portfolio/holdings and just send actions no matter what?
class TradingParams(BaseModel):
    pcpl_usd: int = Field(..., gt=0)
    stake_ratio: float = Field(..., gt=0, le=1)
    evaluation_period: int = Field(..., gt=0)


# TODO: Create report generator
class ReportParams(BaseModel):
    gen: bool


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
        # TODO: Use rel file paths
        path = os.path.join(
            "C:/Users/mguth/Desktop/backtest/config/tests", f"{name}.yaml"
        )
        try:
            with open(path, "r") as f:
                try:
                    test_yml = yaml.safe_load(f)
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
