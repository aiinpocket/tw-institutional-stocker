"""Price schemas."""
from datetime import date
from typing import Optional, List
from pydantic import BaseModel


class StockPriceResponse(BaseModel):
    trade_date: date
    code: str
    name: Optional[str] = None
    market: Optional[str] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[int] = None
    turnover: Optional[int] = None
    change_amount: Optional[float] = None
    change_percent: Optional[float] = None

    class Config:
        from_attributes = True


class PriceHistoryResponse(BaseModel):
    code: str
    name: str
    market: str
    data: List[StockPriceResponse]


class PriceLatestResponse(BaseModel):
    date: date
    total: int
    items: List[StockPriceResponse]
