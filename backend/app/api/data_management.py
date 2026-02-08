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
    id: Optional[int] = None
    name: str
    type: str  # file, table, bucket
    path: str
    size: str
    source: Optional[str] = None
    rows: Optional[int] = 0

class RowUpdate(BaseModel):
    row_id: str
    data: Dict[str, Any]

from sqlmodel import Session, select
from backend.app.core.db import get_session
from backend.app.models.synced_table import SyncedTable
from backend.app.models.audit import AuditLog

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
                        id=None,
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
        asset_type = "table"
        if table.source_type == "minio":
            asset_type = "bucket"
            
        assets.append(DataAsset(
             id=table.id,
             name=table.table_name,
             type=asset_type,
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

def _mysql_quote_ident(name: str) -> str:
    if "`" in name:
        raise HTTPException(status_code=400, detail="Invalid identifier")
    return f"`{name}`"

def _ch_quote_ident(name: str) -> str:
    if "`" in name:
        raise HTTPException(status_code=400, detail="Invalid identifier")
    if "." in name:
        parts = name.split(".")
        return ".".join(f"`{p}`" for p in parts)
    return f"`{name}`"

def _parse_ch_type(ch_type: str) -> str:
    t = ch_type.strip()
    if t.startswith("Nullable(") and t.endswith(")"):
        return t[len("Nullable("):-1]
    return t

def _coerce_ch_value(value: Any, ch_type: str) -> Any:
    if value is None:
        return None
    base = _parse_ch_type(ch_type)
    if isinstance(value, str):
        if value == "":
            return None
        if base.startswith(("Int", "UInt")):
            try:
                return int(value)
            except Exception:
                return value
        if base.startswith(("Float", "Decimal")):
            try:
                return float(value)
            except Exception:
                return value
        if base in ("Bool", "Boolean"):
            lowered = value.lower()
            if lowered in ("true", "1", "yes", "y"):
                return 1
            if lowered in ("false", "0", "no", "n"):
                return 0
            return value
    return value

def _get_clickhouse_columns_and_types(client: Client, table_name: str) -> Dict[str, str]:
    desc = client.execute(f"DESCRIBE {_ch_quote_ident(table_name)}")
    return {row[0]: row[1] for row in desc}

def _pick_rowid_column(columns: List[str]) -> str:
    if "id" in columns:
        return "id"
    return columns[0] if columns else "id"

def _pick_mysql_rowid_column(inspector, table_name: str) -> Optional[str]:
    pk_constraint = inspector.get_pk_constraint(table_name)
    if pk_constraint and pk_constraint.get("constrained_columns"):
        return pk_constraint["constrained_columns"][0]
    cols = [c["name"] for c in inspector.get_columns(table_name)]
    if "id" in cols:
        return "id"
    return None

@router.get("/preview")
def preview_data(path: str, id: Optional[int] = None, limit: int = 20, offset: int = 0, session: Session = Depends(get_session)):
    # Check if file exists (Local File)
    if os.path.exists(path) and id is None:
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
    
    # Check SyncedTable registry
    synced_table = None
    if id:
        synced_table = session.get(SyncedTable, id)
    
    if not synced_table:
        # Fallback to name search if ID not provided (backward compatibility) or not found
        synced_table = session.exec(select(SyncedTable).where(SyncedTable.table_name == path)).first()
    
    if synced_table:
        path = synced_table.table_name # Ensure we use the correct name from DB if fetched by ID
        if synced_table.source_type == 'clickhouse':
            # Fetch from Target ClickHouse
            try:
                client = get_ck_client()
                column_types = _get_clickhouse_columns_and_types(client, path)
                columns = list(column_types.keys())
                rowid_col = _pick_rowid_column(columns)
                # Get Total
                total = client.execute(f"SELECT count(*) FROM {_ch_quote_ident(path)}")[0][0]
                
                # Get Data (ClickHouse doesn't support OFFSET without LIMIT properly, but LIMIT offset, limit works)
                data = client.execute(f"SELECT * FROM {_ch_quote_ident(path)} LIMIT {offset}, {limit}")
                
                # Map to dict
                formatted_data = []
                for row in data:
                    row_dict = dict(zip(columns, row))
                    row_dict["_rowid"] = row_dict.get(rowid_col)
                    formatted_data.append(row_dict)

                return {
                    "columns": columns,
                    "data": formatted_data,
                    "total": total,
                    "meta": {"source": "clickhouse", "editable": True, "rowid_col": rowid_col}
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
                    "total": objs.get('KeyCount', 0), # Approximation or need to count
                    "meta": {"source": "minio", "editable": False}
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
                rowid_col = _pick_mysql_rowid_column(inspector, path)
                
                # Fetch data
                # Construct query. We simulate '_rowid' with the PK.
                if rowid_col:
                    query = text(f"SELECT {_mysql_quote_ident(rowid_col)} as _rowid, * FROM {safe_path} LIMIT {limit} OFFSET {offset}")
                else:
                    query = text(f"SELECT * FROM {safe_path} LIMIT {limit} OFFSET {offset}")
                try:
                    result = conn.execute(query)
                    columns = result.keys()
                    raw_rows = result.fetchall()
                    if "_rowid" in columns:
                        data = [dict(zip(columns, row)) for row in raw_rows]
                        editable = True
                    else:
                        data = []
                        for idx, row in enumerate(raw_rows):
                            row_dict = dict(zip(columns, row))
                            row_dict["_rowid"] = offset + idx
                            data.append(row_dict)
                        editable = False
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
                    editable = False
                
                return {
                    "columns": list(columns),
                    "data": data,
                    "total": total,
                    "meta": {"source": "mysql", "editable": editable, "rowid_col": rowid_col if editable else None}
                }
    except Exception as e:
        print(f"DB Error: {e}")

    raise HTTPException(status_code=404, detail="Asset not found")

@router.delete("/{name_or_id}")
def delete_asset(name_or_id: str, type: Optional[str] = None, session: Session = Depends(get_session)):
    """
    Delete a data asset. 
    name_or_id: Can be a file name or a SyncedTable ID.
    """
    try:
        # 1. Try to find as SyncedTable by ID first (if digit)
        if name_or_id.isdigit():
             synced_table = session.get(SyncedTable, int(name_or_id))
             if synced_table:
                 name = synced_table.table_name
                 # Proceed to delete logic
                 if synced_table.source_type == 'clickhouse':
                    try:
                        client = get_ck_client()
                        client.execute(f"DROP TABLE IF EXISTS {name}")
                    except Exception as ck_err:
                         raise HTTPException(status_code=500, detail=f"ClickHouse Drop Error: {str(ck_err)}")
                
                 elif synced_table.source_type == 'minio':
                    # MinIO deletion logic
                    import boto3
                    from botocore.client import Config
                    try:
                        s3 = boto3.client('s3',
                           endpoint_url=f"http://{settings.MINIO_ENDPOINT}" if not settings.MINIO_ENDPOINT.startswith("http") else settings.MINIO_ENDPOINT,
                           aws_access_key_id=settings.MINIO_ROOT_USER,
                           aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
                           config=Config(signature_version='s3v4')
                        )
                        # List and delete objects
                        paginator = s3.get_paginator('list_objects_v2')
                        pages = paginator.paginate(Bucket=name)
                        for page in pages:
                            if 'Contents' in page:
                                delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
                                s3.delete_objects(Bucket=name, Delete={'Objects': delete_keys})
                        s3.delete_bucket(Bucket=name)
                    except Exception as minio_err:
                         if "NoSuchBucket" not in str(minio_err):
                             raise HTTPException(status_code=500, detail=f"MinIO Delete Error: {str(minio_err)}")
                 else:
                    # MySQL/System DB
                    engine = create_engine(settings.SYSTEM_DB_URL)
                    with engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS `{name}`"))
                        conn.commit()
                 
                 session.delete(synced_table)
                 
                 # Add Audit Log
                 session.add(AuditLog(
                     user_id="user", # TODO: Get actual user
                     action="delete_asset",
                     resource=name,
                     details=f"Deleted asset {name} (ID: {name_or_id}) of type {synced_table.source_type}"
                 ))
                 
                 session.commit()
                 return {"ok": True, "message": f"Asset {name} deleted"}

        # 2. Try to find as file
        name = name_or_id
        if os.path.exists(DATA_DIR):
            file_path = os.path.join(DATA_DIR, name)
            if os.path.exists(file_path) and os.path.isfile(file_path):
                os.remove(file_path)
                # Add Audit Log
                session.add(AuditLog(
                     user_id="user",
                     action="delete_asset",
                     resource=name,
                     details=f"Deleted local file {name}"
                ))
                session.commit()
                return {"ok": True, "message": f"File {name} deleted"}

        # 3. Fallback: Find SyncedTable by Name (Legacy support or if ID lookup failed)
        statement = select(SyncedTable).where(SyncedTable.table_name == name)
        synced_table = session.exec(statement).first()
        
        if synced_table:
             # ... (Deletion Logic) ...
             if synced_table.source_type == 'clickhouse':
                    try:
                        client = get_ck_client()
                        client.execute(f"DROP TABLE IF EXISTS {name}")
                    except: pass
             elif synced_table.source_type == 'minio':
                    pass 
             else:
                    engine = create_engine(settings.SYSTEM_DB_URL)
                    with engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS `{name}`"))
                        conn.commit()
             session.delete(synced_table)
             
             # Add Audit Log
             session.add(AuditLog(
                 user_id="user",
                 action="delete_asset",
                 resource=name,
                 details=f"Deleted asset {name} (Fallback Name Match) of type {synced_table.source_type}"
             ))
             
             session.commit()
             return {"ok": True, "message": f"Table {name} deleted"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=404, detail="Asset not found")

@router.delete("/table/{table_name}/row/{row_id}")
def delete_table_row(table_name: str, row_id: str, session: Session = Depends(get_session)):
    try:
        synced = session.exec(select(SyncedTable).where(SyncedTable.table_name == table_name)).first()
        if synced and synced.source_type == "clickhouse":
            client = get_ck_client()
            column_types = _get_clickhouse_columns_and_types(client, table_name)
            columns = list(column_types.keys())
            if not columns:
                raise HTTPException(status_code=400, detail="Empty ClickHouse table schema")
            rowid_col = _pick_rowid_column(columns)
            coerced_row_id = _coerce_ch_value(row_id, column_types.get(rowid_col, "String"))
            client.execute(
                f"ALTER TABLE {_ch_quote_ident(table_name)} DELETE WHERE {_ch_quote_ident(rowid_col)} = %(row_id)s",
                {"row_id": coerced_row_id},
            )
        else:
            engine = create_engine(settings.SYSTEM_DB_URL)
            inspector = inspect(engine)
            if table_name not in inspector.get_table_names():
                raise HTTPException(status_code=404, detail="Table not found")
            rowid_col = _pick_mysql_rowid_column(inspector, table_name)
            if not rowid_col:
                raise HTTPException(status_code=400, detail="Table has no primary key or id column; cannot delete rows")

            safe_table = _mysql_quote_ident(table_name)
            safe_rowid = _mysql_quote_ident(rowid_col)

            with engine.connect() as conn:
                dialect = engine.dialect.name
                limit_clause = " LIMIT 1" if dialect == "mysql" else ""
                result = conn.execute(
                    text(f"DELETE FROM {safe_table} WHERE {safe_rowid} = :row_id{limit_clause}"),
                    {"row_id": row_id},
                )
                conn.commit()
                if result.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Row not found")
            
        # Add Audit Log
        session.add(AuditLog(
             user_id="user",
             action="delete_row",
             resource=table_name,
             details=f"Deleted row {row_id} from table {table_name}"
        ))
        session.commit()
        return {"ok": True}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/table/{table_name}/row/{row_id}")
def update_table_row(table_name: str, row_id: str, update: RowUpdate, session: Session = Depends(get_session)):
    try:
        if update.row_id != row_id:
            raise HTTPException(status_code=400, detail="row_id mismatch")

        synced = session.exec(select(SyncedTable).where(SyncedTable.table_name == table_name)).first()
        if synced and synced.source_type == "clickhouse":
            client = get_ck_client()
            column_types = _get_clickhouse_columns_and_types(client, table_name)
            columns = list(column_types.keys())
            if not columns:
                raise HTTPException(status_code=400, detail="Empty ClickHouse table schema")
            rowid_col = _pick_rowid_column(columns)
            coerced_row_id = _coerce_ch_value(row_id, column_types.get(rowid_col, "String"))

            set_parts = []
            params: Dict[str, Any] = {"row_id": coerced_row_id}
            for key, value in update.data.items():
                if key in ("_rowid", rowid_col):
                    continue
                if key not in column_types:
                    continue
                set_parts.append(f"{_ch_quote_ident(key)} = %({key})s")
                params[key] = _coerce_ch_value(value, column_types.get(key, "String"))

            if not set_parts:
                return {"ok": True}

            client.execute(
                f"ALTER TABLE {_ch_quote_ident(table_name)} UPDATE {', '.join(set_parts)} WHERE {_ch_quote_ident(rowid_col)} = %(row_id)s",
                params,
            )
        else:
            engine = create_engine(settings.SYSTEM_DB_URL)
            inspector = inspect(engine)
            if table_name not in inspector.get_table_names():
                raise HTTPException(status_code=404, detail="Table not found")

            rowid_col = _pick_mysql_rowid_column(inspector, table_name)
            if not rowid_col:
                raise HTTPException(status_code=400, detail="Table has no primary key or id column; cannot update rows")

            table_cols = {c["name"] for c in inspector.get_columns(table_name)}
            safe_table = _mysql_quote_ident(table_name)
            safe_rowid = _mysql_quote_ident(rowid_col)

            set_clauses = []
            params: Dict[str, Any] = {"row_id": row_id}
            for key, value in update.data.items():
                if key in ("_rowid", rowid_col):
                    continue
                if key not in table_cols:
                    continue
                set_clauses.append(f"{_mysql_quote_ident(key)} = :{key}")
                params[key] = value

            if not set_clauses:
                return {"ok": True}

            query = text(f"UPDATE {safe_table} SET {', '.join(set_clauses)} WHERE {safe_rowid} = :row_id LIMIT 1")
            with engine.connect() as conn:
                dialect = engine.dialect.name
                limit_clause = " LIMIT 1" if dialect == "mysql" else ""
                query = text(f"UPDATE {safe_table} SET {', '.join(set_clauses)} WHERE {safe_rowid} = :row_id{limit_clause}")
                result = conn.execute(query, params)
                conn.commit()
                if result.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Row not found")
            
        # Add Audit Log
        session.add(AuditLog(
             user_id="user",
             action="update_row",
             resource=table_name,
             details=f"Updated row {row_id} in table {table_name}. Fields: {list(update.data.keys())}"
        ))
        session.commit()
        return {"ok": True}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/structure")
def get_structure(path: str, id: Optional[int] = None, session: Session = Depends(get_session)):
    # Check file
    if os.path.exists(path) and id is None:
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
    
    # Check Registry for ClickHouse/MinIO
    synced_table = None
    if id:
        synced_table = session.get(SyncedTable, id)
    if not synced_table:
        synced_table = session.exec(select(SyncedTable).where(SyncedTable.table_name == path)).first()

    if synced_table:
        path = synced_table.table_name # Use correct name
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

@router.get("/download/{name_or_id}")
def download_asset(name_or_id: str, format: str = "csv", session: Session = Depends(get_session)):
    """
    Export data asset to a file.
    """
    try:
        # Audit Log Entry (Before or After?) - Let's log the attempt
        # Resolving name first
        target_name = name_or_id
        
        # 1. Check Registry for Source Type
        synced_table = None
        if name_or_id.isdigit():
             synced_table = session.get(SyncedTable, int(name_or_id))
        
        if not synced_table:
             synced_table = session.exec(select(SyncedTable).where(SyncedTable.table_name == name_or_id)).first()
        
        name = name_or_id
        if synced_table:
             name = synced_table.table_name
             target_name = name

        # Log Download Action
        session.add(AuditLog(
             user_id="user",
             action="download_asset",
             resource=target_name,
             details=f"Exported/Downloaded asset {target_name} in format {format}"
        ))
        session.commit()
        
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
