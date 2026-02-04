from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlmodel import Session, select
from typing import List
from backend.app.core.db import get_session, engine
from backend.app.models.task import DataTask
from backend.app.models.audit import AuditLog
from backend.app.services.spark_service import submit_spark_job
from backend.app.services.sync_service import run_sync_task
import logging

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)

@router.post("/", response_model=DataTask)
def create_task(task: DataTask, session: Session = Depends(get_session)):
    session.add(task)
    session.commit()
    session.refresh(task)
    
    # Audit Log
    log = AuditLog(user_id="admin", action="create_task", resource=task.name, details=f"Type: {task.task_type}")
    session.add(log)
    session.commit()
    
    return task

@router.get("/", response_model=List[DataTask])
def read_tasks(skip: int = 0, limit: int = 100, session: Session = Depends(get_session)):
    tasks = session.exec(select(DataTask).offset(skip).limit(limit)).all()
    return tasks

@router.get("/{task_id}", response_model=DataTask)
def read_task(task_id: int, session: Session = Depends(get_session)):
    task = session.get(DataTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

def run_spark_job_background(task_id: int):
    with Session(engine) as session:
        task = session.get(DataTask, task_id)
        if not task:
            return
        
        try:
            success, output = submit_spark_job(task)
            task.status = "success" if success else "failed"
            
            # Audit Log for completion
            log = AuditLog(user_id="system", action="task_completed", resource=task.name, details=f"Status: {task.status}")
            session.add(log)
            
        except Exception as e:
            logger.error(f"Task {task_id} failed with exception: {e}")
            task.status = "failed"
            
            # Audit Log for failure
            log = AuditLog(user_id="system", action="task_failed", resource=task.name, details=str(e))
            session.add(log)
        
        session.add(task)
        session.commit()

@router.post("/{task_id}/run")
def run_task(task_id: int, background_tasks: BackgroundTasks, session: Session = Depends(get_session)):
    task = session.get(DataTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task.status = "running"
    task.progress = 0
    session.add(task)
    
    # Audit Log for start
    log = AuditLog(user_id="admin", action="run_task", resource=task.name)
    session.add(log)
    
    session.commit()
    
    if task.task_type == "sync":
        background_tasks.add_task(run_sync_task, task_id)
    else:
        background_tasks.add_task(run_spark_job_background, task_id)
    
    return {"message": "Task started", "task_id": task_id}
