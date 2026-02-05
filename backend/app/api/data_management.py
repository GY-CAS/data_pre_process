from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any, Optional
import os
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text
from backend.app.core.config import settings

router = APIRouter(prefix="/data-mgmt", tags=["data-management"])

DATA_DIR = "data"

class DataAsset(BaseModel):
    name: str
    type: str  # file, table
    path: str
    size: str
    source: Optional[str] = None
    rows: Optional[int] = 0

class RowUpdate(BaseModel):
    row_id: int
    data: Dict[str, Any]

from sqlmodel import Session, select
from backend.app.core.db import get_session
from backend.app.models.synced_table import SyncedTable

@router.get("/assets", response_model=List[DataAsset])
def get_assets(session: Session = Depends(get_session)):
    assets = []
    
    # 1. Scan data dir for files
    if os.path.exists(DATA_DIR):
        for root, dirs, files in os.walk(DATA_DIR):
            for file in files:
                if file.endswith(('.csv', '.parquet', '.json')):
                    path = os.path.join(root, file)
                    size = os.path.getsize(path)
                    assets.append(DataAsset(
                        name=file,
                        type="file",
                        path=path,
                        size=f"{size / 1024:.2f} KB",
                        source="Local File",
                        rows=0 
                    ))
    
    # 2. Query Synced Tables from Registry
    synced_tables = session.exec(select(SyncedTable)).all()
    for table in synced_tables:
        assets.append(DataAsset(
            name=table.table_name,
            type="table",
            path=table.table_name, 
            size="-", # Size unknown for DB tables
            source=table.source_type, # Using source_type ('minio', 'clickhouse', 'mysql') as source for UI logic
            rows=table.row_count
        ))
        
    return assets

from clickhouse_driver import Client

def get_ck_client():
    return Client(
        host=settings.CK_HOST, 
        port=settings.CK_PORT, 
        user=settings.CK_USER, 
        password=settings.CK_PASSWORD, 
        database='default'
    )

@router.get("/preview")
def preview_data(path: str, limit: int = 20, offset: int = 0, session: Session = Depends(get_session)):
    # Check if file exists
    if os.path.exists(path):
        try:
            if path.endswith('.csv'):
                df = pd.read_csv(path, nrows=limit, skiprows=lambda x: x > 0 and x < offset) 
                if offset == 0:
                    df = pd.read_csv(path, nrows=limit)
                else:
                    df = pd.read_csv(path, skiprows=range(1, offset+1), nrows=limit)
            elif path.endswith('.parquet'):
                df = pd.read_parquet(path)
                df = df.iloc[offset:offset+limit]
            elif path.endswith('.json'):
                df = pd.read_json(path, orient='records', lines=True)
                df = df.iloc[offset:offset+limit]
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type")
            
            df = df.where(pd.notnull(df), None)
            return {
                "columns": df.columns.tolist(),
                "data": df.to_dict(orient="records"),
                "total": 1000 
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    # Check SyncedTable registry to determine storage location
    synced_table = session.exec(select(SyncedTable).where(SyncedTable.table_name == path)).first()
    
    if synced_table:
        if synced_table.source_type == 'clickhouse':
            # Fetch from Target ClickHouse
            try:
                client = get_ck_client()
                # Get Total
                total = client.execute(f"SELECT count(*) FROM {path}")[0][0]
                
                # Get Data (ClickHouse doesn't support OFFSET without LIMIT properly, but LIMIT offset, limit works)
                data = client.execute(f"SELECT * FROM {path} LIMIT {offset}, {limit}")
                columns = [c[0] for c in client.execute(f"DESCRIBE {path}")]
                
                # Map to dict
                formatted_data = []
                for row in data:
                    # We need a _rowid. ClickHouse doesn't have stable rowid.
                    # Use a fake one or try to find a PK?
                    # For now, fake it based on offset.
                    row_dict = dict(zip(columns, row))
                    row_dict['_rowid'] = 0 # Dummy, or generate
                    formatted_data.append(row_dict)

                return {
                    "columns": columns,
                    "data": formatted_data,
                    "total": total
                }
            except Exception as e:
                 raise HTTPException(status_code=500, detail=f"ClickHouse Error: {str(e)}")
        
        elif synced_table.source_type == 'minio':
             # MinIO doesn't support tabular preview in this way easily if it's a bucket.
             # If it's a single file synced, maybe? 
             # But our sync logic syncs a bucket.
             # So 'path' is a bucket name.
             # We should probably list files or just return a message?
             # Or we can return a list of files as "data"?
             
             import boto3
             from botocore.client import Config
             
             try:
                 s3 = boto3.client('s3',
                    endpoint_url=f"http://{settings.MINIO_ENDPOINT}" if not settings.MINIO_ENDPOINT.startswith("http") else settings.MINIO_ENDPOINT,
                    aws_access_key_id=settings.MINIO_ROOT_USER,
                    aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
                    config=Config(signature_version='s3v4')
                 )
                 
                 # List objects
                 objs = s3.list_objects_v2(Bucket=path, MaxKeys=limit) # Ignore offset for now or implement continuation token logic
                 
                 if 'Contents' not in objs:
                      return {"columns": ["Key", "Size", "LastModified"], "data": [], "total": 0}
                 
                 data = []
                 for obj in objs['Contents']:
                     data.append({
                         "Key": obj['Key'],
                         "Size": obj['Size'],
                         "LastModified": str(obj['LastModified']),
                         "_rowid": obj['ETag'] # Use ETag as ID
                     })
                     
                 return {
                     "columns": ["Key", "Size", "LastModified"],
                     "data": data,
                     "total": objs.get('KeyCount', 0) # Approximation or need to count
                 }
             except Exception as e:
                  raise HTTPException(status_code=500, detail=f"MinIO Preview Error: {str(e)}")

    # Fallback to System MySQL DB
    try:
        engine = create_engine(settings.SYSTEM_DB_URL)
        inspector = inspect(engine)
        if path in inspector.get_table_names():
            with engine.connect() as conn:
                # Use backticks for MySQL table names to handle special characters
                safe_path = f"`{path}`"
                
                # Get total count
                total = conn.execute(text(f"SELECT COUNT(*) FROM {safe_path}")).scalar()
                
                # Get data with rowid
                # MySQL doesn't have a stable 'rowid' like SQLite, but we can use PK or just rely on offsets if no editing.
                # For editing, we need a PK. Let's assume there is an 'id' column or similar.
                # If the table was created by us (pandas to_sql), it might have an 'index' column if index=True, or no PK.
                # For safety, let's try to find the PK.
                pk_col = "id" # Default assumption
                pk_constraint = inspector.get_pk_constraint(path)
                if pk_constraint and pk_constraint['constrained_columns']:
                    pk_col = pk_constraint['constrained_columns'][0]
                
                # Fetch data
                # Construct query. We simulate '_rowid' with the PK.
                query = text(f"SELECT {pk_col} as _rowid, * FROM {safe_path} LIMIT {limit} OFFSET {offset}")
                try:
                    result = conn.execute(query)
                    columns = result.keys()
                    data = [dict(zip(columns, row)) for row in result.fetchall()]
                except Exception as query_err:
                    # Fallback if PK assumption fails or other issue
                    print(f"Query failed: {query_err}. Trying simple select.")
                    query = text(f"SELECT * FROM {safe_path} LIMIT {limit} OFFSET {offset}")
                    result = conn.execute(query)
                    columns = result.keys()
                    # Generate fake rowids for display if needed, but editing won't work well
                    data = []
                    for idx, row in enumerate(result.fetchall()):
                         row_dict = dict(zip(columns, row))
                         row_dict['_rowid'] = offset + idx # Fake ID
                         data.append(row_dict)
                
                return {
                    "columns": list(columns),
                    "data": data,
                    "total": total
                }
    except Exception as e:
        print(f"DB Error: {e}")

    raise HTTPException(status_code=404, detail="Asset not found")

@router.delete("/{name}")
def delete_asset(name: str, session: Session = Depends(get_session)):
    """
    Delete a data asset. 
    If it's a file, delete from disk.
    If it's a synced table, drop the table and remove from registry.
    """
    try:
        # 1. Try to find as file
        if os.path.exists(DATA_DIR):
            file_path = os.path.join(DATA_DIR, name)
            if os.path.exists(file_path) and os.path.isfile(file_path):
                os.remove(file_path)
                return {"ok": True, "message": f"File {name} deleted"}

        # 2. Try to find as SyncedTable
        # Check registry first
        statement = select(SyncedTable).where(SyncedTable.table_name == name)
        synced_table = session.exec(statement).first()
        
        if synced_table:
            # Check if ClickHouse
            if synced_table.source_type == 'clickhouse':
                try:
                    client = get_ck_client()
                    client.execute(f"DROP TABLE IF EXISTS {name}")
                except Exception as ck_err:
                     raise HTTPException(status_code=500, detail=f"ClickHouse Drop Error: {str(ck_err)}")
            
            elif synced_table.source_type == 'minio':
                # For MinIO, 'name' is the bucket name (target_table).
                # Deleting the asset means removing it from our registry.
                # Do we want to delete the ACTUAL BUCKET?
                # Usually "Delete Asset" implies removing the reference.
                # If the user wants to delete the bucket, it's a big operation.
                # Let's assume we just remove the registry entry for MinIO to prevent data loss,
                # unless explicitly requested?
                # The user feedback is: "After deleting minio data card... data still exists in background minio".
                # This implies they EXPECT the data to be deleted.
                # So we should try to delete the bucket or objects?
                # Deleting a non-empty bucket requires deleting all objects first.
                
                import boto3
                from botocore.client import Config
                
                try:
                    s3 = boto3.client('s3',
                       endpoint_url=f"http://{settings.MINIO_ENDPOINT}" if not settings.MINIO_ENDPOINT.startswith("http") else settings.MINIO_ENDPOINT,
                       aws_access_key_id=settings.MINIO_ROOT_USER,
                       aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
                       config=Config(signature_version='s3v4')
                    )
                    
                    # 1. List all objects
                    paginator = s3.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=name)
                    
                    for page in pages:
                        if 'Contents' in page:
                            delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
                            # 2. Delete objects in batches
                            s3.delete_objects(Bucket=name, Delete={'Objects': delete_keys})
                            
                    # 3. Delete the bucket itself
                    s3.delete_bucket(Bucket=name)
                    
                except Exception as minio_err:
                     # If bucket doesn't exist, ignore. Else raise.
                     if "NoSuchBucket" not in str(minio_err):
                         raise HTTPException(status_code=500, detail=f"MinIO Delete Error: {str(minio_err)}")

            else:
                # Default: Drop table from System MySQL DB
                engine = create_engine(settings.SYSTEM_DB_URL)
                with engine.connect() as conn:
                    # Use backticks for safety
                    conn.execute(text(f"DROP TABLE IF EXISTS `{name}`"))
                    conn.commit()
            
            # Remove from registry
            session.delete(synced_table)
            session.commit()
            return {"ok": True, "message": f"Table {name} deleted"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=404, detail="Asset not found")

@router.delete("/table/{table_name}/row/{row_id}")
def delete_table_row(table_name: str, row_id: int):
    try:
        engine = create_engine(settings.SYSTEM_DB_URL)
        # Verify table exists to prevent injection
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
             raise HTTPException(status_code=404, detail="Table not found")
             
        # Find PK
        pk_col = "id"
        pk_constraint = inspector.get_pk_constraint(table_name)
        if pk_constraint and pk_constraint['constrained_columns']:
            pk_col = pk_constraint['constrained_columns'][0]
             
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM {table_name} WHERE {pk_col} = :row_id"), {"row_id": row_id})
            conn.commit()
            return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/table/{table_name}/row/{row_id}")
def update_table_row(table_name: str, row_id: int, update: RowUpdate):
    try:
        engine = create_engine(settings.SYSTEM_DB_URL)
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
             raise HTTPException(status_code=404, detail="Table not found")
             
        # Find PK
        pk_col = "id"
        pk_constraint = inspector.get_pk_constraint(table_name)
        if pk_constraint and pk_constraint['constrained_columns']:
            pk_col = pk_constraint['constrained_columns'][0]

        # Construct UPDATE query dynamically
        set_clauses = []
        params = {"row_id": row_id}
        for key, value in update.data.items():
            if key == "_rowid": continue # Skip ID
            if key == pk_col: continue # Skip PK update for safety
            set_clauses.append(f"{key} = :{key}")
            params[key] = value
            
        if not set_clauses:
            return {"ok": True} # Nothing to update
            
        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_col} = :row_id"
        
        with engine.connect() as conn:
            conn.execute(text(query), params)
            conn.commit()
            return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/structure")
def get_structure(path: str, session: Session = Depends(get_session)):
    # Check file
    if os.path.exists(path):
        try:
            # Read just a bit to get columns and types
            if path.endswith('.csv'):
                df = pd.read_csv(path, nrows=5)
            elif path.endswith('.parquet'):
                df = pd.read_parquet(path)
                df = df.head(5)
            elif path.endswith('.json'):
                 df = pd.read_json(path, orient='records', lines=True, chunksize=5)
                 df = next(df)
            else:
                 raise HTTPException(status_code=400, detail="Unsupported file type")
                 
            structure = []
            for col in df.columns:
                structure.append({
                    "name": col,
                    "type": str(df[col].dtype),
                    "nullable": True 
                })
            return structure
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    # Check Registry for ClickHouse
    synced_table = session.exec(select(SyncedTable).where(SyncedTable.table_name == path)).first()
    if synced_table:
        if synced_table.source_type == 'clickhouse':
            try:
                client = get_ck_client()
                desc = client.execute(f"DESCRIBE {path}")
                # desc row: (name, type, default_type, default_expression, comment, codec_expression, ttl_expression)
                structure = []
                for row in desc:
                     structure.append({
                        "name": row[0],
                        "type": row[1],
                        "nullable": False # CH types are non-nullable by default unless Nullable()
                    })
                return structure
            except Exception as e:
                 raise HTTPException(status_code=500, detail=f"ClickHouse Error: {str(e)}")
        
        elif synced_table.source_type == 'minio':
             # MinIO structure?
             # For a bucket, structure is just Object Metadata usually.
             # Or if we treat it as a file system, name/size/type.
             return [
                 {"name": "Key", "type": "String", "nullable": False},
                 {"name": "Size", "type": "Int64", "nullable": False},
                 {"name": "LastModified", "type": "DateTime", "nullable": False},
                 {"name": "ETag", "type": "String", "nullable": False}
             ]

    # Check DB
    try:
        engine = create_engine(settings.SYSTEM_DB_URL)
        inspector = inspect(engine)
        if path in inspector.get_table_names():
            columns = inspector.get_columns(path)
            structure = []
            for col in columns:
                structure.append({
                    "name": col['name'],
                    "type": str(col['type']),
                    "nullable": col['nullable']
                })
            return structure
    except Exception as e:
        pass
        
    raise HTTPException(status_code=404, detail="Asset not found")

@router.get("/download/{name}")
def download_asset(name: str, format: str = "csv", session: Session = Depends(get_session)):
    """
    Export data asset to a file.
    Supports CSV, Excel, JSON.
    For MinIO, generates a presigned URL.
    """
    try:
        # 1. Check Registry for Source Type
        synced_table = session.exec(select(SyncedTable).where(SyncedTable.table_name == name)).first()
        
        # Determine Data Source & Fetch Data
        df = None
        is_minio = False
        
        if synced_table:
            if synced_table.source_type == 'clickhouse':
                try:
                    client = get_ck_client()
                    data = client.execute(f"SELECT * FROM {name}")
                    columns = [c[0] for c in client.execute(f"DESCRIBE {name}")]
                    df = pd.DataFrame(data, columns=columns)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"ClickHouse Export Error: {str(e)}")
            
            elif synced_table.source_type == 'minio':
                is_minio = True
                # For MinIO, we don't fetch data to DF, we generate a link
                # But 'name' here is the 'table_name' which is the 'target_bucket' in sync logic.
                # If we want to download the whole bucket? 
                # Or specific files?
                # The UI lists "Assets". For MinIO sync, we currently treat 'target_bucket' as the asset name in SyncedTable.
                # So this is downloading a whole bucket? That's complex (zip).
                # Or maybe the user wants to browse files?
                # The prompt says: "MinIO stored data can pop up download link based on minio provided link, set validity to 5min"
                # If the asset represents a file (from local scan), we download it.
                # If it represents a SyncedTable from MinIO, it's a Bucket.
                # Providing a download link for a whole bucket is not standard Presigned URL behavior (it's per object).
                # Let's assume for now we might list objects or zip them? 
                # OR, if the requirement implies downloading a specific file *within* the asset management if we supported browsing.
                # But 'Data Management' currently lists 'Assets' (Tables/Buckets).
                # If I click download on a Bucket asset, maybe I should get a list of links? 
                # Or maybe just one link if it was a single file sync?
                # Let's implement a "List of Presigned URLs" or just fail for now if it's a bucket.
                
                # However, if the asset is a Local File (type='file'), we handle it below.
                pass
                
            else: # MySQL / System DB
                engine = create_engine(settings.SYSTEM_DB_URL)
                df = pd.read_sql(f"SELECT * FROM `{name}`", engine)
        
        # 2. Check Local Files
        elif os.path.exists(os.path.join(DATA_DIR, name)):
             path = os.path.join(DATA_DIR, name)
             if path.endswith('.csv'): df = pd.read_csv(path)
             elif path.endswith('.parquet'): df = pd.read_parquet(path)
             elif path.endswith('.json'): df = pd.read_json(path, orient='records', lines=True)
        
        # Handle MinIO Presigned URL generation
        if is_minio:
             # We need to know which file. 
             # If 'name' is a bucket, we can't generate ONE link.
             # Maybe we assume the asset IS the file if we synced differently?
             # Current sync logic: target_table = target_bucket.
             # So 'name' is a bucket name.
             # We can't download a bucket as a file easily without zipping.
             # Let's try to generate a link for the first object as a demo, or return a list?
             # Requirement: "pop up download link... set validity to 5min"
             # Let's return a special JSON response telling UI to show links?
             
             # Re-read config for MinIO (we need credentials not in settings but in DB? 
             # No, 'SyncedTable' doesn't store creds. We rely on .env or default?
             # The system should have a configured MinIO client.
             # Let's use the one from .env if available, or try to find source creds?
             # We don't store source creds in SyncedTable. 
             # We'll assume the system has access via env vars (MINIO_ROOT_...) as configured in main.
             
             import boto3
             from botocore.client import Config
             
             s3 = boto3.client('s3',
                endpoint_url=f"http://{settings.MINIO_ENDPOINT}" if not settings.MINIO_ENDPOINT.startswith("http") else settings.MINIO_ENDPOINT,
                aws_access_key_id=settings.MINIO_ROOT_USER,
                aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
                config=Config(signature_version='s3v4')
             )
             
             # List objects in bucket
             try:
                 # If name is bucket
                 objs = s3.list_objects_v2(Bucket=name)
                 if 'Contents' not in objs:
                     raise HTTPException(status_code=404, detail="Bucket is empty or not found")
                 
                 # Generate links for all items (limit to 50?)
                 links = []
                 for obj in objs['Contents'][:50]:
                     url = s3.generate_presigned_url('get_object',
                        Params={'Bucket': name, 'Key': obj['Key']},
                        ExpiresIn=300) # 5 mins
                     links.append({"key": obj['Key'], "url": url})
                     
                 return {"status": "minio_links", "links": links}
             except Exception as e:
                 raise HTTPException(status_code=500, detail=f"MinIO Error: {str(e)}")

        # Handle DataFrame Export
        if df is not None:
            from fastapi.responses import StreamingResponse
            import io
            
            stream = io.BytesIO()
            media_type = "text/csv"
            filename = f"{name}.csv"
            
            if format == "csv":
                df.to_csv(stream, index=False)
                media_type = "text/csv"
            elif format == "json":
                df.to_json(stream, orient="records", lines=False)
                media_type = "application/json"
                filename = f"{name}.json"
            elif format == "excel":
                # Requires openpyxl
                df.to_excel(stream, index=False)
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                filename = f"{name}.xlsx"
            else:
                raise HTTPException(status_code=400, detail="Invalid format")
                
            stream.seek(0)
            return StreamingResponse(stream, media_type=media_type, headers={"Content-Disposition": f"attachment; filename={filename}"})

    except Exception as e:
        print(f"Export Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=404, detail="Asset not found")
