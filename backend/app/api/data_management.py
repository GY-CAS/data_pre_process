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
            source=table.source_name,
            rows=table.row_count
        ))
        
    return assets

@router.get("/preview")
def preview_data(path: str, limit: int = 20, offset: int = 0):
    # Check if file exists
    if os.path.exists(path):
        try:
            if path.endswith('.csv'):
                df = pd.read_csv(path, nrows=limit, skiprows=lambda x: x > 0 and x < offset) # simple offset logic for csv is hard without full read, assuming just head for files
                # For files, just read head for now as before
                if offset == 0:
                    df = pd.read_csv(path, nrows=limit)
                else:
                    # simplistic pagination for csv
                    df = pd.read_csv(path, skiprows=range(1, offset+1), nrows=limit)
            elif path.endswith('.parquet'):
                df = pd.read_parquet(path)
                df = df.iloc[offset:offset+limit]
            elif path.endswith('.json'):
                # JSON pagination is hard without reading all
                df = pd.read_json(path, orient='records', lines=True)
                df = df.iloc[offset:offset+limit]
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type")
            
            # Convert to JSON friendly format
            df = df.where(pd.notnull(df), None)
            return {
                "columns": df.columns.tolist(),
                "data": df.to_dict(orient="records"),
                "total": 1000 # Mock for files
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    # If not file, try DB Table
    try:
        engine = create_engine(settings.SYSTEM_DB_URL)
        inspector = inspect(engine)
        if path in inspector.get_table_names():
            with engine.connect() as conn:
                # Get total count
                total = conn.execute(text(f"SELECT COUNT(*) FROM {path}")).scalar()
                
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
                query = text(f"SELECT {pk_col} as _rowid, * FROM {path} LIMIT {limit} OFFSET {offset}")
                try:
                    result = conn.execute(query)
                    columns = result.keys()
                    data = [dict(zip(columns, row)) for row in result.fetchall()]
                except Exception as query_err:
                    # Fallback if PK assumption fails or other issue
                    print(f"Query failed: {query_err}. Trying simple select.")
                    query = text(f"SELECT * FROM {path} LIMIT {limit} OFFSET {offset}")
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
def delete_asset(name: str):
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
        engine = create_engine(settings.SYSTEM_DB_URL)
        with Session(engine) as session:
            # Check registry first
            statement = select(SyncedTable).where(SyncedTable.table_name == name)
            synced_table = session.exec(statement).first()
            
            if synced_table:
                # Drop table from DB
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
                    conn.commit()
                
                # Remove from registry
                session.delete(synced_table)
                session.commit()
                return {"ok": True, "message": f"Table {name} deleted"}
            
            # If not in registry but exists in DB (orphan?), try to drop it anyway if requested?
            # For safety, we only delete what we track or what is clearly a file in our data dir.
            
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
def get_structure(path: str):
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
