"""Broker schemas."""
from datetime import date
from typing import Optional, List
from pydantic import BaseModel


class BrokerTradeResponse(BaseModel):
    trade_date: date
    code: str
    name: Optional[str] = None
    broker_name: str
    broker_id: Optional[str] = None
    buy_vol: int
    sell_vol: int
    net_vol: int
    pct: Optional[float] = None
    rank: Optional[int] = None
    side: Optional[str] = None

    class Config:
        from_attributes = True


class BrokerTradesListResponse(BaseModel):
    date: date
    total: int
    items: List[BrokerTradeResponse]


class BrokerHistoryResponse(BaseModel):
    broker_name: str
    data: List[BrokerTradeResponse]
