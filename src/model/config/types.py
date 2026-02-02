from enum import Enum


class DatasourceType(str, Enum):
    YAHOO_FINANCE = "yfinance"
    CSV_DATASET = "csv"
