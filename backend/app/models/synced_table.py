from typing import Optional
from sqlmodel import Field, SQLModel, Index
from datetime import datetime

class SyncedTable(SQLModel, table=True):
    __tablename__ = "synced_tables"
    __table_args__ = (
        Index('idx_synced_tables_table_name', 'table_name'),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True, nullable=False)
    table_name: str = Field(max_length=255)
    source_type: str = Field(max_length=50)
    source_name: str = Field(max_length=255)
    row_count: int = Field(default=0)
    data_type: Optional[str] = Field(default=None, max_length=50)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
