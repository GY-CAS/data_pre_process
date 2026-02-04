from typing import Optional
from sqlmodel import Field, SQLModel
from datetime import datetime

class SyncedTable(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    table_name: str = Field(index=True, unique=True)
    source_type: str
    source_name: str
    row_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
