from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func

Base = declarative_base()

class StockOHLCV(Base):
    __tablename__ = "stock_ohlcv"

    id = Column(Integer, primary_key=True, index=True)
    date_time = Column(DateTime, nullable=False, index=True)
    symbol = Column(String(10), nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    created_at = Column(DateTime, server_default=func.now())