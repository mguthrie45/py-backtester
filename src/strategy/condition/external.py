from pathlib import Path

from pandas.core.frame import DataFrame
from collections import defaultdict
from importlib.machinery import SourceFileLoader
from typing import Any, Literal

from pydantic import BaseModel, FilePath, model_validator, Field

from log.logger import Logger
from metrics.slice.types import StockSliceMetricsName
from model.data.trade_slices import TradeAction
from model.data.state_slices import CapitalStateSliceWindow
from model.data.state_slices import HoldingStateSliceWindow
from model.data.state_slices import StockSliceWindow
from strategy.condition.types import ConditionType


class ContextAttrs(BaseModel):
    slice_lookback_window_size: int = Field(default=0, ge=0)
    include_portfolio: bool = False
    include_holdings: bool = False


class ExternalCondition(BaseModel):
    name: str
    type: Literal[ConditionType.EXTERNAL] = Field(default=ConditionType.EXTERNAL.value)
    metric_attrs: list[StockSliceMetricsName] = Field(default=[])
    py_file: FilePath

    context_attrs: ContextAttrs

    @model_validator(mode="after")
    def load_module(self):
        assert Path(self.py_file).exists(), (
            f"condition '{self.py_file}' does not exist."
        )

        self._module = SourceFileLoader(self.name, str(self.py_file)).load_module()

        return self

    @property
    def lookback_window_size(self) -> int:
        """Lookback (slices) required by this condition's context."""
        return self.context_attrs.slice_lookback_window_size

    def eval(
        self,
        stock_slices: StockSliceWindow,
        capital_state_slices: CapitalStateSliceWindow,
        holding_state_slices: defaultdict[str, HoldingStateSliceWindow],
    ) -> TradeAction | None:
        stock_slices_df = stock_slices.df
        capital_state_slices_df = capital_state_slices.df
        holding_state_slices_df = defaultdict[str, DataFrame | list](
            list, {ticker: window.df for ticker, window in holding_state_slices.items()}
        )

        try:
            Logger.debug('executing handler: "%s"', self.py_file)
            out = self._module.handler(
                stock_slices_df, capital_state_slices_df, holding_state_slices_df
            )

            Logger.debug("executed:\n out: %s", out)

            if out:
                return TradeAction.from_tuple(out, dt=stock_slices.slices[-1].dt)

            return None
        except Exception as err:
            raise Exception(
                f'issue executing the handler for condition: "{self.name}", error: {err}'
            )
