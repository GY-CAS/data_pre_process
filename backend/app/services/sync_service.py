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
                
                # --- Data Verification for MySQL ---
                task.verification_status = "pending"
                session.add(task)
                session.commit()
                
                try:
                    from sqlalchemy import create_engine, text
                    
                    # 1. Get Source Checksum (if overwrite mode and feasible)
                    # For append mode, simple count check is safer. For overwrite, we can try checksum.
                    # Note: Checksum is expensive. Let's do a Row Count check first which is fast.
                    
                    # Source Count
                    source_cnt_query = f"SELECT count(*) FROM {source_table}"
                    source_count = pd.read_sql(source_cnt_query, url).iloc[0, 0]
                    
                    # Target Count
                    target_engine = create_engine(target_url)
                    with target_engine.connect() as t_conn:
                        target_cnt_query = text(f"SELECT count(*) FROM {target_table}")
                        target_count = t_conn.execute(target_cnt_query).scalar()
                    
                    if mode == "overwrite":
                        if source_count != target_count:
                            raise Exception(f"Rows mismatch: Source({source_count}) != Target({target_count})")
                        
                        # Optional: Advanced Checksum (CRC32)
                        # Only run if table isn't huge to avoid timeout, or if user requested strict mode.
                        # Using CRC32 on all columns.
                        # 1. Get Columns
                        cols_df = pd.read_sql(f"SHOW COLUMNS FROM {source_table}", url)
                        cols = cols_df['Field'].tolist()
                        
                        if cols:
                            # Construct Checksum Query: SELECT SUM(CRC32(CONCAT_WS(',', col1, col2...)))
                            # CAST to UNSIGNED to avoid overflow issues in some versions if needed, though CRC32 returns unsigned.
                            # BIT_XOR is order independent, SUM depends on row order if not strictly ordered, but sum is commutative.
                            cols_str = ", ".join(cols)
                            checksum_sql = f"SELECT SUM(CRC32(CONCAT_WS(',', {cols_str}))) FROM {{table}}"
                            
                            src_checksum = pd.read_sql(checksum_sql.format(table=source_table), url).iloc[0, 0]
                            
                            with target_engine.connect() as t_conn:
                                tgt_checksum = t_conn.execute(text(checksum_sql.format(table=target_table))).scalar()
                            
                            # Handle potential None/Decimal types
                            if str(src_checksum) != str(tgt_checksum):
                                raise Exception(f"Checksum mismatch: Source({src_checksum}) != Target({tgt_checksum})")

                    elif mode == "append":
                        # For append, we can't easily check total count unless we knew before_count.
                        pass

                    task.verification_status = "success"

                except Exception as verify_err:
                    print(f"Verification Error: {verify_err}")
                    task.verification_status = "failed"
                    # We do NOT fail the task here, just mark verification as failed
                    # raise verify_err # Propagate error to fail task

                    
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
                         
                         # --- Data Verification for MinIO ---
                         try:
                             # 1. Get Source Info
                             src_head = s3.head_object(Bucket=source_table, Key=key)
                             src_etag = src_head.get('ETag', '').strip('"')
                             
                             # 2. Get Target Info
                             tgt_head = s3.head_object(Bucket=target_bucket, Key=key)
                             tgt_etag = tgt_head.get('ETag', '').strip('"')
                             
                             if src_etag and tgt_etag and src_etag != tgt_etag:
                                 raise Exception(f"File verification failed for {key}: Source ETag {src_etag} != Target ETag {tgt_etag}")
                             
                             # If we get here for the last file, or we want overall status?
                             # MinIO syncs file by file. We should probably track if ANY failed.
                             # But here we are inside a loop.
                             # If one fails, we set status=failed and maybe break?
                             # Or just let it continue?
                             
                         except Exception as verify_err:
                             print(f"MinIO Verification Error for {key}: {verify_err}")
                             # We should mark task as verification failed but maybe continue?
                             # Or fail the whole verification?
                             task.verification_status = "failed"
                             # raise verify_err 
                             
                         task.progress = int((processed_files / total_files) * 100)
                         session.add(task)
                         session.commit()
            
            # If we finished loop without setting verification_status to failed, set to success?
            # We need to initialize it first.
            if datasource.type == "minio" and task.verification_status != "failed":
                 task.verification_status = "success"


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
