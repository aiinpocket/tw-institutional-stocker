from sqlalchemy import Column, Integer, String, BigInteger, Date, Numeric, Boolean, ForeignKey, UniqueConstraint, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True)
    code = Column(String(10), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    market = Column(String(10), nullable=False)  # 'TWSE' or 'TPEX'
    industry = Column(String(50), index=True)  # 產業別
    total_shares = Column(BigInteger)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships
    flows = relationship("InstitutionalFlow", back_populates="stock", cascade="all, delete-orphan")
    holdings = relationship("ForeignHolding", back_populates="stock", cascade="all, delete-orphan")
    prices = relationship("StockPrice", back_populates="stock", cascade="all, delete-orphan")
    ratios = relationship("InstitutionalRatio", back_populates="stock", cascade="all, delete-orphan")
    broker_trades = relationship("BrokerTrade", back_populates="stock", cascade="all, delete-orphan")
    baselines = relationship("InstitutionalBaseline", back_populates="stock", cascade="all, delete-orphan")


class InstitutionalFlow(Base):
    __tablename__ = "institutional_flows"
    __table_args__ = (UniqueConstraint('stock_id', 'trade_date'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False, index=True)
    foreign_net = Column(BigInteger, default=0)
    trust_net = Column(BigInteger, default=0)
    dealer_net = Column(BigInteger, default=0)
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="flows")


class ForeignHolding(Base):
    __tablename__ = "foreign_holdings"
    __table_args__ = (UniqueConstraint('stock_id', 'trade_date'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False, index=True)
    total_shares = Column(BigInteger)
    foreign_shares = Column(BigInteger)
    foreign_ratio = Column(Numeric(8, 4))
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="holdings")


class StockPrice(Base):
    __tablename__ = "stock_prices"
    __table_args__ = (UniqueConstraint('stock_id', 'trade_date'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False, index=True)
    open_price = Column(Numeric(12, 2))
    high_price = Column(Numeric(12, 2))
    low_price = Column(Numeric(12, 2))
    close_price = Column(Numeric(12, 2))
    volume = Column(BigInteger)
    turnover = Column(BigInteger)
    change_amount = Column(Numeric(10, 2))
    change_percent = Column(Numeric(8, 4))
    transactions = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="prices")


class InstitutionalRatio(Base):
    __tablename__ = "institutional_ratios"
    __table_args__ = (UniqueConstraint('stock_id', 'trade_date'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False, index=True)
    foreign_ratio = Column(Numeric(8, 4))
    trust_ratio_est = Column(Numeric(8, 4))
    dealer_ratio_est = Column(Numeric(8, 4))
    three_inst_ratio_est = Column(Numeric(8, 4))
    trust_shares_est = Column(BigInteger)
    dealer_shares_est = Column(BigInteger)
    change_5d = Column(Numeric(8, 4))
    change_20d = Column(Numeric(8, 4))
    change_60d = Column(Numeric(8, 4))
    change_120d = Column(Numeric(8, 4))
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="ratios")


class BrokerTrade(Base):
    __tablename__ = "broker_trades"

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False, index=True)
    broker_name = Column(String(100), nullable=False)
    broker_id = Column(String(50))
    buy_vol = Column(BigInteger, default=0)
    sell_vol = Column(BigInteger, default=0)
    net_vol = Column(BigInteger, default=0)
    pct = Column(Numeric(8, 4))
    rank = Column(Integer)
    side = Column(String(10))  # 'buy' or 'sell'
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="broker_trades")


class InstitutionalBaseline(Base):
    __tablename__ = "institutional_baselines"
    __table_args__ = (UniqueConstraint('stock_id', 'baseline_date'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    baseline_date = Column(Date, nullable=False)
    trust_shares_base = Column(BigInteger)
    dealer_shares_base = Column(BigInteger)
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="baselines")
