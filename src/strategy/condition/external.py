import os
from importlib.machinery import SourceFileLoader
from typing import Literal

from pydantic import BaseModel, FilePath, model_validator, Field

from controller.portfolio import PortfolioController
from log.logger import Logger
from metrics.slice.types import StockSliceMetricsName
from model.data.Action import Action
from model.data.StockSlice import StockSlice
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
    def file_exists(self):
        assert os.path.exists(self.py_file), (
            f"condition '{self.py_file}' does not exist."
        )
        return self

    def args(self) -> list[str]:
        pass

    def lookback_window_size(self) -> int:
        """Lookback (slices) required by this condition's context."""
        return self.context_attrs.slice_lookback_window_size

    def eval(
        self, slices_with_lookback: StockSlice, portfolio: PortfolioController
    ) -> Action | None:
        try:
            Logger.debug('executing handler: "%s"', self.py_file)
            out = (
                SourceFileLoader(self.name, str(self.py_file))
                .load_module()
                .handler(slices_with_lookback, portfolio)
            )

            Logger.debug("executed:\n out: %s", out)

            if out:
                return Action(out)

            return None
        except Exception as err:
            raise Exception(
                f'issue executing the handler for condition: "{self.name}", error: {err}'
            )
