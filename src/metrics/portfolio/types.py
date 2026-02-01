from enum import Enum


class PortfolioMetricsName(str, Enum):
    BOUGHT = "bought"
    RETURNS = "returns"
    DEBT = "debt"
