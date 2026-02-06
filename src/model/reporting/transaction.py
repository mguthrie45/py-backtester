from pydantic import BaseModel
from model.data.Action import Action
from model.data.StockSlice import StockSlice
from model.portfolio.slice import PortfolioSlice


class Transaction(BaseModel):
    action: Action
    stock_slice: StockSlice
    portfolio_slice: PortfolioSlice
