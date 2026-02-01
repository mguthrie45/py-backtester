import os
from typing import Annotated, Union

import yaml
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    computed_field,
)

from controller.portfolio import PortfolioController
from log.logger import Logger
from metrics.slice.types import (
    StockSliceMetricsName_Extended,
)
from model.data.Action import Action
from model.data.StockSlice import StockSlice
from strategy.condition.expression import ExpressionCondition
from strategy.condition.external import ExternalCondition

type Condition = Union[ExpressionCondition, ExternalCondition]


class Params(BaseModel):
    stop_loss: float = Field(..., gt=0, le=1)
    conditions: list[Annotated[Condition, Field(discriminator="type")]]


class Strategy(BaseModel):
    name: str
    params: Params

    def eval(
        self, slices_with_lookback: list[StockSlice], portfolio: PortfolioController
    ) -> list[Action]:
        Logger.debug(
            "Evaluating strategy against slice: \n%s", slices_with_lookback[-1]
        )

        return list(
            filter(
                lambda action: action is not None,
                [
                    cond.eval(slices_with_lookback, portfolio)
                    for cond in self.params.conditions
                ],
            )
        )

    @computed_field
    def extended_metrics(self) -> list[StockSliceMetricsName_Extended]:
        metric_names: set[StockSliceMetricsName_Extended] = set()

        for cond in self.params.conditions:
            for attr in cond.metric_attrs:
                if attr in StockSliceMetricsName_Extended:
                    metric_names.add(attr)

        return list(metric_names)

    @property
    def max_lookback(self) -> int:
        """Max lookback (slices) needed across all conditions (e.g. RSI_10 needs 10)."""
        if not self.params.conditions:
            return 0
        return int(max(cond.lookback_window_size() for cond in self.params.conditions))

    @staticmethod
    def from_yaml(name: str):
        # TODO: Parameterize file paths
        path = os.path.join(
            "C:/Users/mguth/Desktop/backtest/config/strategies/", f"{name}.yaml"
        )
        Logger.debug("Loading strategy: name=%s, filepath=%s", name, path)

        try:
            with open(path, "r") as f:
                Logger.debug("Opened strategy file.")
                try:
                    strat_yml = yaml.safe_load(f)
                    Logger.debug("Loaded strategy file as yaml.")
                    try:
                        Logger.debug("Parsing strategy file.")
                        return Strategy.model_validate(strat_yml)
                    except ValidationError as err:
                        print(f"Strategy '{name}' is misconfigured.")
                        raise err

                except yaml.YAMLError as err:
                    print(err)
                    raise err

        except FileNotFoundError as err:
            print(f"Strategy '{name}' not found.")
            raise err
