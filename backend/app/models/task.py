from typing import Optional
from sqlmodel import Field, SQLModel, Index
from datetime import datetime

class DataTask(SQLModel, table=True):
    __tablename__ = "data_tasks"
    __table_args__ = (
        Index('idx_data_tasks_status', 'status'),
        Index('idx_data_tasks_name', 'name'),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True, nullable=False)
    name: str = Field(max_length=255)
    task_type: str = Field(max_length=50)
    config: str = Field(default="{}")
    status: str = Field(default="pending", max_length=20)
    verification_status: Optional[str] = Field(default=None, max_length=20)
    progress: int = Field(default=0)
    spark_app_id: Optional[str] = Field(default=None, max_length=100)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default=None)
