from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel


class BiometricDataRequest(BaseModel):
    user_id: str
    device_id: Optional[str] = None
    heart_rate: Optional[float] = None
    sleep_data: Optional[Dict[str, Any]] = None
    activity: Optional[Dict[str, Any]] = None
    recorded_at: Optional[datetime] = None


class BiometricDataResponse(BaseModel):
    id: str
    user_id: str
    device_id: Optional[str]
    heart_rate: Optional[float]
    sleep_data: Optional[Dict[str, Any]]
    activity: Optional[Dict[str, Any]]
    recorded_at: datetime
    created_at: datetime

