from typing import Literal, Optional

from pydantic import BaseModel, computed_field
from controller import PortfolioManager
from log.logger import Logger
from metrics.portfolio.types import PortfolioMetricsName
from metrics.slice.types import (
    StockSliceMetricsName,
    StockSliceMetricsName_Extended,
    StockSliceExtendedMetricsNameBatchExtensionSizeAdapter,
)
from model.data.trade_slices import TradeAction
from model.data.state_slices import StockSlice
from strategy.condition.types import (
    ConditionType,
    TradeActionPolicy,
    CONDITION_SLICE_ACCESSOR,
    CONDITION_PORTFOLIO_ACCESSOR,
)


class ExpressionCondition(BaseModel):
    name: str
    type: Literal[ConditionType.EXPRESSION] = ConditionType.EXPRESSION.value
    action_policy: TradeActionPolicy
    metric_attrs: list[StockSliceMetricsName]
    portfolio_attrs: Optional[list[PortfolioMetricsName]] = None
    # TODO: Add validation to enforce safety
    # TODO: Add validation to check unused metrics, undefined metrics
    expr: str
    action: TradeAction

    @computed_field
    @property
    def expr_compiled(self) -> str:
        res = self.expr

        for metric_attr in self.metric_attrs:
            slice_metric_ref = f"{CONDITION_SLICE_ACCESSOR}.{metric_attr.value}"
            res = res.replace(metric_attr.value, slice_metric_ref)

        if self.portfolio_attrs:
            for portfolio_attr in self.portfolio_attrs:
                portfolio_attr_ref = (
                    f"{CONDITION_PORTFOLIO_ACCESSOR}.{portfolio_attr.value}"
                )
                res = res.replace(portfolio_attr.value, portfolio_attr_ref)

        return res

    @property
    def lookback_window_size(self) -> int:
        """Max lookback (slices) needed by extended metrics used in this condition."""
        extended = [
            attr for attr in self.metric_attrs if attr in StockSliceMetricsName_Extended
        ]
        if not extended:
            return 0
        return max(
            StockSliceExtendedMetricsNameBatchExtensionSizeAdapter.get_batch_extension_size(
                attr
            ).value
            for attr in extended
        )

    def eval(
        self, stock_slices: list[StockSlice], portfolio: PortfolioManager
    ) -> TradeAction | None:
        Logger.debug(
            "Evaluating expression condition '%s'\n action_policy='%s'\n expr='%s'\n compiled='%s'",
            self.name,
            self.action_policy.value,
            self.expr,
            self.expr_compiled,
        )
        stock_slice = stock_slices[-1]

        is_cond_curr = eval(
            self.expr_compiled,
            globals={
                CONDITION_SLICE_ACCESSOR: stock_slice,
                CONDITION_PORTFOLIO_ACCESSOR: portfolio,
            },
        )
        Logger.debug("is_cond_curr=%s", is_cond_curr)

        is_curr_bool = isinstance(is_cond_curr, bool)

        if self.action_policy == TradeActionPolicy.CROSSOVER:
            is_cond_prev = (
                True  # We do not want to choose an action based on unknown information
                if not stock_slice.prev
                else eval(
                    self.expr_compiled,
                    globals={CONDITION_SLICE_ACCESSOR: stock_slice.prev},
                )
            )
            Logger.debug("is_cond_prev=%s", is_cond_prev)

            is_prev_bool = isinstance(is_cond_prev, bool)

            if is_curr_bool and is_prev_bool:
                return self.action if is_cond_curr and not is_cond_prev else None
            else:
                raise Exception(
                    f"condition expression '{self.expr}' does not return a boolean."
                )
        elif self.action_policy == TradeActionPolicy.ABSOLUTE:
            if is_curr_bool:
                return self.action if is_cond_curr else None
            else:
                raise Exception(
                    f"condition expression '{self.expr}' does not return a boolean."
                )
        else:
            raise Exception(f"action policy '{self.action_policy.value}' is unknown.")
