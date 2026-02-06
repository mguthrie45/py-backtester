from pydantic import BaseModel, Field

from model.data.Holding import Holding


class PortfolioSlice(BaseModel):
    bought: float = Field(ge=0)
    debt: float = Field(ge=0)
    holdings: list[Holding]
    value: float
