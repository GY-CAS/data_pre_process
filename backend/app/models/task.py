from typing import Optional
from sqlmodel import Field, SQLModel
from datetime import datetime

class DataTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    task_type: str  # full_sync, preprocess
    config: str  # JSON string containing source, target, operators
    status: str = Field(default="pending")  # pending, running, success, failed
    verification_status: Optional[str] = Field(default=None) # verified, failed, None
    progress: int = Field(default=0)
    spark_app_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default=None) # Initially None until run
