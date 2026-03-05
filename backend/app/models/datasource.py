from typing import Optional
from sqlmodel import Field, SQLModel
from datetime import datetime

class DataSource(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = Field(default=None)
    type: str  # mysql, clickhouse, minio, etc.
    data_type: Optional[str] = Field(default=None)  # IMAGE, TIMESERIES, NER
    connection_info: str  # JSON string
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
