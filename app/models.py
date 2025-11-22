from pydantic import BaseModel, Field
from datetime import datetime

class Heartbeat(BaseModel):
    device_id: str = Field(..., example="device_a")
    user_id: str = Field(..., example="user_123")
    timestamp: datetime = Field(..., example="2024-01-15T10:00:00Z")
    heart_rate: int = Field(..., ge=30, le=220, example=75) #validate range 30-220
