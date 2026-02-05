from fastapi import APIRouter, Depends, Query, HTTPException
from sqlmodel import Session, select, func, col
from typing import List, Optional, Dict, Any
from backend.app.core.db import get_session
from backend.app.models.audit import AuditLog

router = APIRouter(prefix="/audit", tags=["audit"])

@router.get("/", response_model=Dict[str, Any])
def get_audit_logs(
    skip: int = 0,
    limit: int = 100,
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    resource: Optional[str] = None,
    session: Session = Depends(get_session)
):
    query = select(AuditLog)
    if user_id:
        query = query.where(AuditLog.user_id == user_id)
    if action:
        query = query.where(AuditLog.action == action)
    if resource:
        query = query.where(AuditLog.resource == resource)
    
    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total = session.exec(count_query).one()
    
    # Get items
    query = query.offset(skip).limit(limit).order_by(AuditLog.timestamp.desc())
    items = session.exec(query).all()
    
    return {"items": items, "total": total}

@router.delete("/")
def delete_audit_logs(ids: List[int], session: Session = Depends(get_session)):
    if not ids:
        return {"ok": True, "count": 0}
        
    statement = select(AuditLog).where(col(AuditLog.id).in_(ids))
    logs = session.exec(statement).all()
    
    for log in logs:
        session.delete(log)
    
    session.commit()
    return {"ok": True, "count": len(logs)}

@router.post("/", response_model=AuditLog)
def create_audit_log(log: AuditLog, session: Session = Depends(get_session)):
    session.add(log)
    session.commit()
    session.refresh(log)
    return log
