from datetime import datetime
from enum import Enum
from uuid import uuid4
from pydantic import BaseModel, Field


class HoldingType(str, Enum):
    BUY = "buy"
    SHORT = "short"


class Holding(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    type: HoldingType
    purchased_dt: datetime
    buy_price: float
    num_shares: int
