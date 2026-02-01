from enum import Enum

CONDITION_SLICE_ACCESSOR = "stock_slice"
CONDITION_PORTFOLIO_ACCESSOR = "portfolio"
# TODO: Implement holding level accessor
CONDITION_HOLDING_ACCESSOR = "holding"


class ConditionType(str, Enum):
    EXPRESSION = "expression"
    EXTERNAL = "external"


class ActionPolicy(str, Enum):
    ABSOLUTE = "absolute"
    CROSSOVER = "crossover"