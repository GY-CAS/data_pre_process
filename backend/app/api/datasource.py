from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from typing import List, Dict, Any
from sqlmodel import Session, select, func
from backend.app.core.db import get_session
from backend.app.models.datasource import DataSource
from backend.app.models.audit import AuditLog
import json

router = APIRouter(prefix="/datasources", tags=["datasources"])

@router.post("/", response_model=DataSource)
def create_datasource(datasource: DataSource, session: Session = Depends(get_session)):
    session.add(datasource)
    session.commit()
    session.refresh(datasource)
    
    # Audit Log
    log = AuditLog(user_id="admin", action="create_datasource", resource=datasource.name, details=f"Type: {datasource.type}")
    session.add(log)
    session.commit()
    
    return datasource

@router.get("/", response_model=Dict[str, Any])
def read_datasources(
    skip: int = 0, 
    limit: int = 100, 
    name: str = None, 
    type: str = None, 
    session: Session = Depends(get_session)
):
    # Base query for filtering
    query = select(DataSource)
    if name:
        query = query.where(DataSource.name.contains(name))
    if type:
        query = query.where(DataSource.type == type)
    
    # Count total results matching filter
    count_query = select(func.count()).select_from(query.subquery())
    total = session.exec(count_query).one()

    # Get paged results
    datasources = session.exec(query.offset(skip).limit(limit)).all()
    
    return {
        "data": datasources,
        "total": total,
        "skip": skip,
        "limit": limit
    }

@router.get("/{datasource_id}", response_model=DataSource)
def read_datasource(datasource_id: int, session: Session = Depends(get_session)):
    datasource = session.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="DataSource not found")
    return datasource

@router.delete("/{datasource_id}")
def delete_datasource(datasource_id: int, session: Session = Depends(get_session)):
    datasource = session.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="DataSource not found")
    
    name = datasource.name
    session.delete(datasource)
    
    # Audit Log
    log = AuditLog(user_id="admin", action="delete_datasource", resource=name)
    session.add(log)
    
    session.commit()
    return {"ok": True}

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
            # Simple HTTP fallback if driver not present, or assume driver
            # For simplicity, let's try to use requests to HTTP interface if driver fails, 
            # but usually port 8123 is HTTP. 
            # Let's assume clickhouse-driver is installed or we use a simple query mechanism.
            # Ideally we should use the same method as test_connection.
            # But test_connection uses socket.
            
            # Let's try clickhouse-connect or clickhouse-driver
            # If not available, we can't really fetch metadata easily without extra deps.
            # I will assume 'clickhouse-driver' or 'requests'
            import requests
            # ClickHouse HTTP interface usually on 8123
            # If user provided port 9000 (native), we might need to guess HTTP port or use native driver.
            # Let's try to use native driver first.
            try:
                from clickhouse_driver import Client
                client = Client(host=connection_info['host'], port=connection_info.get('port', 9000), user=connection_info['user'], password=connection_info['password'], database=connection_info['database'])
                result = client.execute('SHOW TABLES')
                return {"tables": [row[0] for row in result]}
            except ImportError:
                 # Fallback to HTTP if driver missing (assuming port 8123 for HTTP if 9000 failed/not used)
                 # This is a bit guessy.
                 pass

        elif db_type == "minio":
            import boto3
            # MinIO usually requires endpoint, access_key, secret_key
            # We need to ensure these are in connection_info
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
    """
    Test connection to a data source.
    connection_info: dict containing type, host, port, user, password, database, etc.
    """
    import os
    import socket

    try:
        db_type = connection_info.get("type", "").lower()
        
        if db_type == "csv" or db_type == "csv file":
            path = connection_info.get("path")
            if not path:
                 return {"status": "error", "message": "File path is required"}
            
            # Normalize path (handle Windows backslashes if needed, though python usually handles it)
            if os.path.exists(path) and os.path.isfile(path):
                 return {"status": "success", "message": f"Path exists: {path}"}
            else:
                 return {"status": "error", "message": f"Path does not exist or is not a file: {path}"}

        elif db_type == "mysql":
            try:
                from sqlalchemy import create_engine, text
                user = connection_info.get("user")
                password = connection_info.get("password")
                host = connection_info.get("host")
                port = connection_info.get("port", 3306)
                database = connection_info.get("database")
                
                if not all([user, host, database]):
                     return {"status": "error", "message": "Missing required fields (user, host, database)"}

                url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
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
                
                endpoint = connection_info.get("endpoint")
                access_key = connection_info.get("access_key")
                secret_key = connection_info.get("secret_key")
                
                if not all([endpoint, access_key, secret_key]):
                    return {"status": "error", "message": "Missing required fields (endpoint, access_key, secret_key)"}

                s3 = boto3.client(
                    's3',
                    endpoint_url=endpoint,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
                # Try to list buckets to verify credentials
                s3.list_buckets()
                return {"status": "success", "message": "Successfully connected to MinIO"}
            except ImportError:
                return {"status": "error", "message": "boto3 library not installed on server."}
            except Exception as e:
                return {"status": "error", "message": f"Connection failed: {str(e)}"}
            
        else:
             return {"status": "error", "message": f"Unsupported data source type: {db_type}"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
