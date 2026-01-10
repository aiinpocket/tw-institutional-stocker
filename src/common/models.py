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
    margin_trading = relationship("MarginTrading", back_populates="stock", cascade="all, delete-orphan")
    revenues = relationship("MonthlyRevenue", back_populates="stock", cascade="all, delete-orphan")


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


class MarginTrading(Base):
    """融資融券資料表"""
    __tablename__ = "margin_trading"
    __table_args__ = (UniqueConstraint('stock_id', 'trade_date'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False, index=True)
    # 融資
    margin_buy = Column(BigInteger, default=0)  # 融資買進
    margin_sell = Column(BigInteger, default=0)  # 融資賣出
    margin_cash_repay = Column(BigInteger, default=0)  # 融資現金償還
    margin_balance = Column(BigInteger, default=0)  # 融資餘額
    margin_limit = Column(BigInteger, default=0)  # 融資限額
    # 融券
    short_sell = Column(BigInteger, default=0)  # 融券賣出
    short_buy = Column(BigInteger, default=0)  # 融券買進(回補)
    short_stock_repay = Column(BigInteger, default=0)  # 融券現券償還
    short_balance = Column(BigInteger, default=0)  # 融券餘額
    short_limit = Column(BigInteger, default=0)  # 融券限額
    # 資券互抵
    offset = Column(BigInteger, default=0)  # 資券互抵
    # 計算欄位
    margin_utilization = Column(Numeric(8, 4))  # 融資使用率
    short_utilization = Column(Numeric(8, 4))  # 融券使用率
    short_margin_ratio = Column(Numeric(8, 4))  # 券資比
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="margin_trading")


class MonthlyRevenue(Base):
    """月營收資料表"""
    __tablename__ = "monthly_revenue"
    __table_args__ = (UniqueConstraint('stock_id', 'year', 'month'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    year = Column(Integer, nullable=False)  # 西元年
    month = Column(Integer, nullable=False)  # 月份 1-12
    revenue = Column(BigInteger)  # 當月營收（千元）
    mom_change = Column(Numeric(15, 4))  # 月增率 (%) - 允許極端值
    yoy_change = Column(Numeric(15, 4))  # 年增率 (%) - 允許極端值
    cumulative_revenue = Column(BigInteger)  # 年累計營收
    cumulative_yoy_change = Column(Numeric(15, 4))  # 年累計年增率 - 允許極端值
    created_at = Column(DateTime, server_default=func.now())

    stock = relationship("Stock", back_populates="revenues")
