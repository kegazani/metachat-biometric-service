from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func as sql_func
from datetime import datetime, date
import uuid

from src.infrastructure.models import BiometricData, BiometricSummary
from src.infrastructure.database import Database


class BiometricRepository:
    def __init__(self, db: Database):
        self.db = db
    
    async def save_biometric_data(
        self, session: AsyncSession, user_id: str, device_id: Optional[str],
        heart_rate: Optional[float], sleep_data: Optional[dict],
        activity: Optional[dict], recorded_at: datetime
    ) -> BiometricData:
        biometric = BiometricData(
            id=str(uuid.uuid4()),
            user_id=user_id,
            device_id=device_id,
            heart_rate=heart_rate,
            sleep_data=sleep_data,
            activity=activity,
            recorded_at=recorded_at
        )
        session.add(biometric)
        await session.commit()
        await session.refresh(biometric)
        return biometric
    
    async def get_biometric_data_by_user(
        self, session: AsyncSession, user_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[BiometricData]:
        query = select(BiometricData).where(BiometricData.user_id == user_id)
        
        if start_date:
            query = query.where(BiometricData.recorded_at >= start_date)
        if end_date:
            query = query.where(BiometricData.recorded_at <= end_date)
        
        query = query.order_by(BiometricData.recorded_at.desc()).limit(limit)
        
        result = await session.execute(query)
        return list(result.scalars().all())
    
    async def get_or_create_daily_summary(
        self, session: AsyncSession, user_id: str, summary_date: str
    ) -> BiometricSummary:
        result = await session.execute(
            select(BiometricSummary).where(
                and_(
                    BiometricSummary.user_id == user_id,
                    BiometricSummary.date == summary_date
                )
            )
        )
        existing = result.scalar_one_or_none()
        
        if existing:
            return existing
        
        new_summary = BiometricSummary(
            id=str(uuid.uuid4()),
            user_id=user_id,
            date=summary_date,
            data_points=0
        )
        session.add(new_summary)
        await session.commit()
        await session.refresh(new_summary)
        return new_summary
    
    async def update_daily_summary(
        self, session: AsyncSession, summary: BiometricSummary,
        avg_heart_rate: Optional[float] = None,
        total_sleep_hours: Optional[float] = None,
        total_steps: Optional[int] = None,
        total_calories: Optional[float] = None,
        data_points: Optional[int] = None
    ) -> BiometricSummary:
        if avg_heart_rate is not None:
            summary.avg_heart_rate = avg_heart_rate
        if total_sleep_hours is not None:
            summary.total_sleep_hours = total_sleep_hours
        if total_steps is not None:
            summary.total_steps = total_steps
        if total_calories is not None:
            summary.total_calories = total_calories
        if data_points is not None:
            summary.data_points = data_points
        
        await session.commit()
        await session.refresh(summary)
        return summary

