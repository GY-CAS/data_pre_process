from typing import Optional
from sqlmodel import Field, SQLModel
from datetime import datetime

class AuditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str
    action: str
    resource: str
    details: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
