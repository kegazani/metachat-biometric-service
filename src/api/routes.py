from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
import structlog

from src.domain.biometric_data import BiometricDataRequest, BiometricDataResponse
from src.api.state import app_state

logger = structlog.get_logger()

router = APIRouter(prefix="/biometric", tags=["biometric"])


@router.post("/data", response_model=BiometricDataResponse)
async def receive_biometric_data(data: BiometricDataRequest):
    try:
        db = app_state.get("db")
        repository = app_state.get("repository")
        kafka_producer = app_state.get("kafka_producer")
        
        if not db or not repository or not kafka_producer:
            raise HTTPException(status_code=503, detail="Service not ready")
        
        recorded_at = data.recorded_at or datetime.utcnow()
        
        async for session in db.get_session():
            biometric = await repository.save_biometric_data(
                session=session,
                user_id=data.user_id,
                device_id=data.device_id,
                heart_rate=data.heart_rate,
                sleep_data=data.sleep_data,
                activity=data.activity,
                recorded_at=recorded_at
            )
            
            kafka_producer.publish_biometric_data_received(
                user_id=data.user_id,
                heart_rate=data.heart_rate,
                sleep_data=data.sleep_data,
                activity=data.activity,
                device_id=data.device_id,
                timestamp=recorded_at
            )
            
            logger.info("Biometric data received", user_id=data.user_id, device_id=data.device_id)
            
            return BiometricDataResponse(
                id=biometric.id,
                user_id=biometric.user_id,
                device_id=biometric.device_id,
                heart_rate=biometric.heart_rate,
                sleep_data=biometric.sleep_data,
                activity=biometric.activity,
                recorded_at=biometric.recorded_at,
                created_at=biometric.created_at
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error receiving biometric data", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/{user_id}", response_model=List[BiometricDataResponse])
async def get_biometric_data(
    user_id: str,
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000)
):
    try:
        db = app_state.get("db")
        repository = app_state.get("repository")
        
        if not db or not repository:
            raise HTTPException(status_code=503, detail="Service not ready")
        
        async for session in db.get_session():
            biometrics = await repository.get_biometric_data_by_user(
                session=session,
                user_id=user_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
            
            return [
                BiometricDataResponse(
                    id=b.id,
                    user_id=b.user_id,
                    device_id=b.device_id,
                    heart_rate=b.heart_rate,
                    sleep_data=b.sleep_data,
                    activity=b.activity,
                    recorded_at=b.recorded_at,
                    created_at=b.created_at
                )
                for b in biometrics
            ]
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting biometric data", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

