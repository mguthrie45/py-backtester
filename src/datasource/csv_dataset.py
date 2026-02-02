from typing import Iterator
from pandas import DataFrame
from datasource.abstract_datasource import AbstractDatasource
from model.config.Test import Test
from strategy.Strategy import Strategy


class CSVDatasetDatasource(AbstractDatasource):
    def __init__(self, test: Test, strategy: Strategy):
        super().__init__(test, strategy)

    def _load_window(self) -> DataFrame:
        pass

    def df_batches(self) -> Iterator[DataFrame]:
        pass
