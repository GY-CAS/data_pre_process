from typing import Optional
from sqlmodel import Field, SQLModel, Index
from datetime import datetime

class AuditLog(SQLModel, table=True):
    __tablename__ = "audit_logs"
    __table_args__ = (
        Index('idx_audit_logs_timestamp', 'timestamp'),
        Index('idx_audit_logs_user_id', 'user_id'),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True, nullable=False)
    user_id: str = Field(max_length=100)
    action: str = Field(max_length=100)
    resource: str = Field(max_length=255)
    details: Optional[str] = Field(default=None)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
