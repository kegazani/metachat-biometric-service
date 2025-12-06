from sqlalchemy import Column, String, Float, Integer, DateTime, JSON, Index
from sqlalchemy.sql import func
from datetime import datetime

from src.infrastructure.database import Base


class BiometricData(Base):
    __tablename__ = "biometric_data"
    
    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    device_id = Column(String, nullable=True)
    heart_rate = Column(Float, nullable=True)
    sleep_data = Column(JSON, nullable=True)
    activity = Column(JSON, nullable=True)
    recorded_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    __table_args__ = (
        Index("idx_biometric_user_recorded", "user_id", "recorded_at"),
    )


class BiometricSummary(Base):
    __tablename__ = "biometric_summary"
    
    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    date = Column(String, nullable=False)
    avg_heart_rate = Column(Float, nullable=True)
    total_sleep_hours = Column(Float, nullable=True)
    total_steps = Column(Integer, nullable=True)
    total_calories = Column(Float, nullable=True)
    data_points = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    __table_args__ = (
        Index("idx_biometric_summary_user_date", "user_id", "date", unique=True),
    )

