from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, select
from typing import List, Optional
from backend.app.core.db import get_session
from backend.app.models.audit import AuditLog

router = APIRouter(prefix="/audit", tags=["audit"])

@router.get("/", response_model=List[AuditLog])
def get_audit_logs(
    skip: int = 0,
    limit: int = 100,
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    session: Session = Depends(get_session)
):
    query = select(AuditLog)
    if user_id:
        query = query.where(AuditLog.user_id == user_id)
    if action:
        query = query.where(AuditLog.action == action)
    query = query.offset(skip).limit(limit).order_by(AuditLog.timestamp.desc())
    return session.exec(query).all()

@router.post("/", response_model=AuditLog)
def create_audit_log(log: AuditLog, session: Session = Depends(get_session)):
    session.add(log)
    session.commit()
    session.refresh(log)
    return log
