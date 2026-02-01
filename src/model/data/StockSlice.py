from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class StockSlice(BaseModel):
    dt: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    prev: Optional["StockSlice"]

    rsi_10: Optional[float] = None
    sma_10: Optional[float] = None
    sma_50: Optional[float] = None

    def __str__(self):
        return f" dt: {self.dt.isoformat()}  close: {self.close}"
