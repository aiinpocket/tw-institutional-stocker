"""Stock schemas."""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class StockBase(BaseModel):
    code: str
    name: str
    market: str
    total_shares: Optional[int] = None
    is_active: bool = True


class StockResponse(StockBase):
    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class StockListResponse(BaseModel):
    total: int
    items: List[StockResponse]
