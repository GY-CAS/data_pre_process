import pandas as pd
import json
import time
from sqlmodel import Session, select
from backend.app.core.db import engine
from backend.app.models.task import DataTask
from backend.app.models.datasource import DataSource
from backend.app.models.synced_table import SyncedTable
from backend.app.core.config import settings
import traceback
import io
from datetime import datetime

def run_sync_task(task_id: int):
    with Session(engine) as session:
        task = session.get(DataTask, task_id)
        if not task:
            return

        try:
            task.status = "running"
            task.progress = 0
            session.add(task)
            session.commit()
            
            config = json.loads(task.config)
            source_id = config.get("source_id")
            target_table = config.get("target", {}).get("table")
            mode = config.get("target", {}).get("mode", "append")
            source_table = config.get("source", {}).get("table") 
            
            # Fetch Source Info
            datasource = session.get(DataSource, source_id)
            if not datasource:
                raise Exception("DataSource not found")
                
            conn_info = json.loads(datasource.connection_info)
            target_url = settings.SYSTEM_DB_URL
            
            total_rows_synced = 0
            
            if datasource.type == "mysql":
                url = f"mysql+pymysql://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}:{conn_info['port']}/{conn_info['database']}"
                
                # Get count
                try:
                    count_query = f"SELECT count(*) FROM {source_table}"
                    total_rows = pd.read_sql(count_query, url).iloc[0, 0]
                except:
                    total_rows = 1000 # Fallback
                
                # Read in chunks
                chunk_size = 5000
                chunks = pd.read_sql(f"SELECT * FROM {source_table}", url, chunksize=chunk_size)
                
                rows_processed = 0
                first_chunk = True
                
                # Handle initial overwrite
                if mode == "overwrite":
                    # We can't easily overwrite with chunks unless we drop table first
                    # Or use 'replace' on first chunk
                    pass 

                for chunk in chunks:
                    current_if_exists = "replace" if (first_chunk and mode == "overwrite") else "append"
                    
                    chunk.to_sql(target_table, target_url, if_exists=current_if_exists, index=False)
                    
                    rows_processed += len(chunk)
                    first_chunk = False
                    
                    # Update Progress
                    progress = int((rows_processed / total_rows) * 100) if total_rows > 0 else 0
                    if progress > 100: progress = 99
                    task.progress = progress
                    session.add(task)
                    session.commit()
                
                total_rows_synced = rows_processed
                    
            elif datasource.type == "clickhouse":
                 from clickhouse_driver import Client
                 client = Client(host=conn_info['host'], port=conn_info.get('port', 9000), user=conn_info['user'], password=conn_info['password'], database=conn_info['database'])
                 
                 data = client.execute(f"SELECT * FROM {source_table}")
                 columns = [c[0] for c in client.execute(f"DESCRIBE {source_table}")]
                 df = pd.DataFrame(data, columns=columns)
                 
                 df.to_sql(target_table, target_url, if_exists=mode, index=False)
                 total_rows_synced = len(df)
                 
            elif datasource.type == "minio":
                 import boto3
                 s3 = boto3.client(
                    's3',
                    endpoint_url=conn_info.get('endpoint'), 
                    aws_access_key_id=conn_info.get('access_key'),
                    aws_secret_access_key=conn_info.get('secret_key')
                 )
                 
                 # Target MinIO bucket from .env (via settings) or user input?
                 # User input 'target_table' is now treated as 'target_bucket_name'
                 target_bucket = target_table
                 
                 # Create target bucket if not exists
                 import botocore.exceptions

                 try:
                     s3.head_bucket(Bucket=target_bucket)
                 except botocore.exceptions.ClientError:
                     # Bucket does not exist or no access, try to create
                     s3.create_bucket(Bucket=target_bucket)
                 
                 # If source_table implies a bucket
                 objects = s3.list_objects_v2(Bucket=source_table)
                 if 'Contents' in objects:
                     total_files = len(objects['Contents'])
                     processed_files = 0
                     
                     for obj in objects['Contents']:
                         key = obj['Key']
                         # Simply copy objects from source bucket to target bucket
                         copy_source = {'Bucket': source_table, 'Key': key}
                         s3.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=key)
                         
                         processed_files += 1
                         # For MinIO sync, row count is not applicable, maybe use file count or bytes?
                         # Let's count files as rows for now or just 0
                         total_rows_synced += 1 
                         
                         task.progress = int((processed_files / total_files) * 100)
                         session.add(task)
                         session.commit()

            # Update SyncedTable Registry
            existing_table = session.exec(select(SyncedTable).where(SyncedTable.table_name == target_table)).first()
            if existing_table:
                if mode == "overwrite":
                    existing_table.row_count = total_rows_synced
                else:
                    existing_table.row_count += total_rows_synced
                existing_table.updated_at = datetime.utcnow()
                session.add(existing_table)
            else:
                new_table = SyncedTable(
                    table_name=target_table,
                    source_type=datasource.type,
                    source_name=datasource.name,
                    row_count=total_rows_synced
                )
                session.add(new_table)
            
            task.status = "success"
            task.progress = 100
            session.add(task)
            session.commit()
            
        except Exception as e:
            traceback.print_exc()
            task.status = "failed"
            # We could store error in a new field or audit log
            session.add(task)
            session.commit()
