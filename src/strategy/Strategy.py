from collections import defaultdict
from typing import Annotated, Tuple, Union

import yaml
from constants import FILE_PATH_CONFIG_STRATEGIES_DIR
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    computed_field,
)

from log.logger import Logger
from metrics.slice.types import (
    StockSliceMetricsName_Extended,
)
from model.data.trade_slices import TradeAction
from model.data.state_slices import CapitalStateSlice, CapitalStateSliceWindow
from model.data.state_slices import HoldingStateSlice, HoldingStateSliceWindow
from model.data.state_slices import StockSlice, StockSliceWindow
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
        self,
        *args: Tuple[
            list[StockSlice],
            list[CapitalStateSlice],
            defaultdict[str, list[HoldingStateSlice]],
        ],
    ) -> list[TradeAction]:
        Logger.debug("Evaluating strategy against slice: \n%s", args[0][-1])

        return list(
            filter(
                lambda action: action is not None,
                [
                    cond.eval(
                        *self.__get_cond_eval_args(
                            cond,
                            *args,
                        )
                    )
                    for cond in self.params.conditions
                ],
            )
        )

    def __get_cond_eval_args(
        self,
        cond: Condition,
        stock_slices: list[StockSlice],
        capital_state_slices: list[CapitalStateSlice],
        holdings_state_slices: defaultdict[str, list[HoldingStateSlice]],
    ) -> tuple:
        stock_slice_window = StockSliceWindow(
            slices=stock_slices[-cond.lookback_window_size :]
        )

        capital_state_slice_window = CapitalStateSliceWindow(
            slices=capital_state_slices[-cond.lookback_window_size :]
        )

        holdings_state_slice_windows = defaultdict[str, HoldingStateSliceWindow](
            None,
            {
                ticker: HoldingStateSliceWindow(slices=slices)
                for ticker, slices in holdings_state_slices.items()
            },
        )

        return (
            stock_slice_window,
            capital_state_slice_window,
            holdings_state_slice_windows,
        )

    @computed_field
    def extended_metrics(self) -> list[StockSliceMetricsName_Extended]:
        metric_names: set[StockSliceMetricsName_Extended] = set()

        for cond in self.params.conditions:
            for attr in cond.metric_attrs:
                if attr in StockSliceMetricsName_Extended:
                    metric_names.add(attr)

        return list[StockSliceMetricsName_Extended](metric_names)

    @property
    def max_lookback(self) -> int:
        """Max lookback (slices) needed across all conditions (e.g. RSI_10 needs 10)."""
        if not self.params.conditions:
            return 0
        return int(max(cond.lookback_window_size for cond in self.params.conditions))

    @staticmethod
    def from_yaml(name: str):
        path = FILE_PATH_CONFIG_STRATEGIES_DIR / f"{name}.yaml"
        Logger.debug("Loading strategy: name=%s, filepath=%s", name, path)

        try:
            content = path.read_text(encoding="utf-8")
            Logger.debug("Opened strategy file.")
            try:
                strat_yml = yaml.safe_load(content)
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
