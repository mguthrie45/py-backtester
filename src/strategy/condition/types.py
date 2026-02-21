from enum import Enum

CONDITION_SLICE_ACCESSOR = "stock_slice"
CONDITION_PORTFOLIO_ACCESSOR = "portfolio"
CONDITION_HOLDING_ACCESSOR = "holding"


class ConditionType(str, Enum):
    EXPRESSION = "expression"
    EXTERNAL = "external"


class TradeActionPolicy(str, Enum):
    ABSOLUTE = "absolute"
    CROSSOVER = "crossover"
