"""Ranking schemas."""
from typing import List, Optional
from pydantic import BaseModel


class RankingItem(BaseModel):
    code: str
    name: str
    market: str
    three_inst_ratio: Optional[float] = None
    change: Optional[float] = None

    class Config:
        from_attributes = True


class RankingResponse(BaseModel):
    window: int
    direction: str  # "up" or "down"
    total: int
    items: List[RankingItem]
