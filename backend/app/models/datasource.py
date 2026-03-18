from typing import Optional
from sqlmodel import Field, SQLModel, Index
from datetime import datetime

class DataSource(SQLModel, table=True):
    __tablename__ = "data_sources"
    __table_args__ = (
        Index('idx_data_sources_name', 'name'),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True, nullable=False)
    name: str = Field(max_length=255)
    description: Optional[str] = Field(default=None, max_length=1000)
    type: str = Field(max_length=50)
    data_type: Optional[str] = Field(default=None, max_length=50)
    connection_info: str = Field(default="{}")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
