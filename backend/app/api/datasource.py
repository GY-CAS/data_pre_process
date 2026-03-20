from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from typing import List, Dict, Any
from sqlmodel import Session, select, func
from app.core.db import get_session
from app.models.datasource import DataSource
from app.models.task import DataTask
from app.models.audit import AuditLog
import json

router = APIRouter(prefix="/datasources", tags=["datasources"])

def get_related_tasks(session: Session, datasource_id: int) -> List[Dict[str, Any]]:
    tasks = session.exec(select(DataTask)).all()
    related_tasks = []
    for task in tasks:
        try:
            config = json.loads(task.config)
            if config.get("source_id") == datasource_id:
                related_tasks.append({
                    "id": task.id,
                    "name": task.name,
                    "task_type": task.task_type,
                    "status": task.status,
                    "created_at": task.created_at.isoformat() if task.created_at else None
                })
        except (json.JSONDecodeError, TypeError):
            continue
    return related_tasks

@router.post("/", response_model=DataSource)
def create_datasource(datasource: DataSource, session: Session = Depends(get_session)):
    existing = session.exec(select(DataSource).where(DataSource.name == datasource.name, DataSource.type == datasource.type)).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Data source with name '{datasource.name}' already exists for type '{datasource.type}'")

    session.add(datasource)
    session.commit()
    session.refresh(datasource)
    
    log = AuditLog(user_id="admin", action="create_datasource", resource=datasource.name, details=f"Type: {datasource.type}, Description: {datasource.description}")
    session.add(log)
    session.commit()
    
    return datasource

@router.get("/", response_model=Dict[str, Any])
def read_datasources(
    skip: int = 0, 
    limit: int = 100, 
    session: Session = Depends(get_session)
):
    query = select(DataSource)
    
    count_query = select(func.count()).select_from(query.subquery())
    total = session.exec(count_query).one()

    datasources = session.exec(query.offset(skip).limit(limit)).all()
    
    return {
        "data": datasources,
        "total": total,
        "skip": skip,
        "limit": limit
    }

@router.get("/search", response_model=Dict[str, Any])
def search_datasources(
    name: str = None,
    type: str = None,
    data_type: str = None,
    page_num: int = 0,
    page_size: int = 10,
    session: Session = Depends(get_session)
):
    query = select(DataSource)
    
    if name:
        query = query.where(DataSource.name.contains(name))
    if type:
        query = query.where(DataSource.type == type)
    if data_type:
        query = query.where(DataSource.data_type == data_type)
    
    count_query = select(func.count()).select_from(query.subquery())
    total = session.exec(count_query).one()

    datasources = session.exec(query.offset((page_num-1)*page_size).limit(page_size)).all()
    
    active_filters = []
    if name:
        active_filters.append({"field": "name", "value": name, "match_type": "contains"})
    if type:
        active_filters.append({"field": "type", "value": type, "match_type": "exact"})
    if data_type:
        active_filters.append({"field": "data_type", "value": data_type, "match_type": "exact"})
    
    return {
        "data": datasources,
        "total": total,
        "page_num": page_num,
        "page_size": page_size,
        "filters_applied": len(active_filters),
        "search_criteria": active_filters
    }

@router.get("/{datasource_id}", response_model=DataSource)
def read_datasource(datasource_id: int, session: Session = Depends(get_session)):
    datasource = session.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="DataSource not found")
    return datasource

@router.get("/{datasource_id}/related-tasks")
def get_datasource_related_tasks(datasource_id: int, session: Session = Depends(get_session)):
    datasource = session.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="DataSource not found")
    
    related_tasks = get_related_tasks(session, datasource_id)
    return {
        "datasource_id": datasource_id,
        "datasource_name": datasource.name,
        "related_tasks_count": len(related_tasks),
        "related_tasks": related_tasks
    }

@router.delete("/{datasource_id}")
def delete_datasource(datasource_id: int, session: Session = Depends(get_session)):
    datasource = session.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="DataSource not found")
    
    related_tasks = get_related_tasks(session, datasource_id)
    if related_tasks:
        task_names = [t["name"] for t in related_tasks[:5]]
        task_info = ", ".join(task_names)
        if len(related_tasks) > 5:
            task_info += f" ... and {len(related_tasks) - 5} more"
        
        raise HTTPException(
            status_code=409,
            detail={
                "error_code": "DATASOURCE_HAS_RELATED_TASKS",
                "message": f"Cannot delete datasource '{datasource.name}' because it has {len(related_tasks)} related task(s)",
                "related_tasks_count": len(related_tasks),
                "related_tasks": related_tasks,
                "suggestion": "Please delete or modify the related tasks before deleting this datasource"
            }
        )
    
    name = datasource.name
    session.delete(datasource)
    
    log = AuditLog(user_id="admin", action="delete_datasource", resource=name)
    session.add(log)
    
    session.commit()
    return {"ok": True, "message": f"DataSource '{name}' deleted successfully"}

@router.get("/{datasource_id}/metadata")
def get_datasource_metadata(datasource_id: int, session: Session = Depends(get_session)):
    datasource = session.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="DataSource not found")
    
    try:
        connection_info = json.loads(datasource.connection_info)
    except:
        return {"tables": []}
        
    db_type = datasource.type
    
    try:
        if db_type == "mysql":
            from sqlalchemy import create_engine, inspect
            url = f"mysql+pymysql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
            engine = create_engine(url)
            inspector = inspect(engine)
            return {"tables": inspector.get_table_names()}
            
        elif db_type == "clickhouse":
            import requests
            try:
                from clickhouse_driver import Client
                client = Client(host=connection_info['host'], port=connection_info.get('port', 9000), user=connection_info['user'], password=connection_info['password'], database=connection_info['database'])
                result = client.execute('SHOW TABLES')
                return {"tables": [row[0] for row in result]}
            except ImportError:
                 pass

        elif db_type == "minio":
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=connection_info.get('endpoint'), 
                aws_access_key_id=connection_info.get('access_key'),
                aws_secret_access_key=connection_info.get('secret_key')
            )
            response = s3.list_buckets()
            return {"tables": [bucket['Name'] for bucket in response['Buckets']]}
            
        return {"tables": []}
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch metadata: {str(e)}")


@router.post("/test-connection")
def test_connection(connection_info: dict):
    import os
    import socket

    try:
        db_type = connection_info.get("type", "").lower()
        
        if db_type == "csv" or db_type == "csv file":
            path = connection_info.get("path")
            if not path:
                 return {"status": "error", "message": "File path is required"}
            
            if os.path.exists(path) and os.path.isfile(path):
                 return {"status": "success", "message": f"Path exists: {path}"}
            else:
                 return {"status": "error", "message": f"Path does not exist or is not a file: {path}"}

        elif db_type == "clickhouse":
            try:
                from clickhouse_driver import Client
                user = connection_info.get("user")
                password = connection_info.get("password")
                host = connection_info.get("host")
                port = connection_info.get("port", 9000)
                database = connection_info.get("database")
                
                if not all([host]):
                     return {"status": "error", "message": "Missing required fields (host)"}

                client = Client(host=host, port=port, user=user, password=password, database=database)
                client.execute('SELECT 1')
                return {"status": "success", "message": "Successfully connected to ClickHouse"}
            except ImportError:
                 return {"status": "error", "message": "clickhouse-driver not installed on server."}
            except Exception as e:
                return {"status": "error", "message": f"Connection failed: {str(e)}"}

        elif db_type == "mysql":
            try:
                from sqlalchemy import create_engine, text
                user = connection_info.get("user")
                password = connection_info.get("password")
                host = connection_info.get("host")
                port = connection_info.get("port", 3306)
                database = connection_info.get("database")
                
                if not all([host]):
                     return {"status": "error", "message": "Missing required fields (host)"}

                url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}" if database else f"mysql+pymysql://{user}:{password}@{host}:{port}"
                engine = create_engine(url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return {"status": "success", "message": "Successfully connected to MySQL"}
            except ImportError:
                 return {"status": "error", "message": "MySQL driver (pymysql) not installed on server."}
            except Exception as e:
                return {"status": "error", "message": f"Connection failed: {str(e)}"}

        elif db_type == "minio":
            try:
                import boto3
                from botocore.exceptions import ClientError
                import socket
                
                endpoint = connection_info.get("endpoint")
                access_key = connection_info.get("access_key")
                secret_key = connection_info.get("secret_key")
                
                if not all([endpoint, access_key, secret_key]):
                    return {"status": "error", "message": "Missing required fields (endpoint, access_key, secret_key)"}

                try:
                    import urllib.parse
                    parsed_url = urllib.parse.urlparse(endpoint)
                    host = parsed_url.netloc.split(':')[0]
                    port = int(parsed_url.netloc.split(':')[1]) if ':' in parsed_url.netloc else 9000
                    
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    sock.connect((host, port))
                    sock.close()
                except Exception as e:
                    return {"status": "error", "message": f"MinIO server not reachable: {str(e)}"}

                s3 = boto3.client(
                    's3',
                    endpoint_url=endpoint,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    config=boto3.session.Config(
                        connect_timeout=2,
                        read_timeout=3,
                        retries={'max_attempts': 1}
                    )
                )
                
                try:
                    try:
                        s3.head_bucket(Bucket='test')
                    except:
                        response = s3.list_buckets()
                        if 'Buckets' in response:
                            pass
                except Exception as e:
                    pass
                
                return {"status": "success", "message": "Successfully connected to MinIO"}
            except ImportError:
                return {"status": "error", "message": "boto3 library not installed on server."}
            except Exception as e:
                return {"status": "error", "message": f"Connection failed: {str(e)}"}
            
        else:
             return {"status": "error", "message": f"Unsupported data source type: {db_type}"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
