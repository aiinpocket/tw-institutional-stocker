"""Institutional data schemas."""
from datetime import date
from typing import Optional, List
from pydantic import BaseModel


class InstitutionalFlowResponse(BaseModel):
    trade_date: date
    code: str
    name: str
    market: str
    foreign_net: int
    trust_net: int
    dealer_net: int

    class Config:
        from_attributes = True


class ForeignHoldingResponse(BaseModel):
    trade_date: date
    code: str
    name: str
    market: str
    total_shares: Optional[int] = None
    foreign_shares: Optional[int] = None
    foreign_ratio: Optional[float] = None

    class Config:
        from_attributes = True


class InstitutionalRatioResponse(BaseModel):
    trade_date: date
    code: str
    name: str
    market: str
    foreign_ratio: Optional[float] = None
    trust_ratio_est: Optional[float] = None
    dealer_ratio_est: Optional[float] = None
    three_inst_ratio_est: Optional[float] = None
    change_5d: Optional[float] = None
    change_20d: Optional[float] = None
    change_60d: Optional[float] = None
    change_120d: Optional[float] = None

    class Config:
        from_attributes = True


class InstitutionalHistoryResponse(BaseModel):
    code: str
    name: str
    market: str
    data: List[InstitutionalRatioResponse]
