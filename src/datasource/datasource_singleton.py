from datasource.abstract_datasource import AbstractDatasource
from datasource.csv_dataset import CSVDatasetDatasource
from datasource.yfinance import YFinanceDatasource
from model.config.Test import Test
from model.config.types import DatasourceType
from strategy.Strategy import Strategy


class Datasource:
    __instance: AbstractDatasource

    def __init__(self, test: Test, strategy: Strategy):
        match test.data_params.datasource_type:
            case DatasourceType.YAHOO_FINANCE:
                self.__instance = YFinanceDatasource(test, strategy)
            case DatasourceType.CSV_DATASET:
                self.__instance = CSVDatasetDatasource(test, strategy)
            case _:
                raise Exception(f"Uknown datasource type {type}")

    @property
    def instance(self) -> AbstractDatasource:
        return self.__instance
