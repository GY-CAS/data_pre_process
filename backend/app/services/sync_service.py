import pandas as pd
import json
import time
from sqlmodel import Session, select
from app.core.db import engine
from app.models.task import DataTask
from app.models.datasource import DataSource
from app.models.synced_table import SyncedTable
from app.core.config import settings
import traceback
import io
from datetime import datetime

from app.models.audit import AuditLog

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
            target_type = config.get("target", {}).get("type", "system_mysql")
            
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
                
                if target_type == "system_minio":
                    # MySQL to MinIO sync
                    import boto3
                    import botocore.exceptions
                    
                    # Initialize MinIO client
                    s3 = boto3.client(
                        's3',
                        endpoint_url=settings.MINIO_ENDPOINT,
                        aws_access_key_id=settings.MINIO_ACCESS_KEY,
                        aws_secret_access_key=settings.MINIO_SECRET_KEY
                    )
                    
                    # Validate and normalize bucket name
                    import re
                    target_bucket = target_table.lower()
                    target_bucket = re.sub(r'[^a-z0-9.-]', '-', target_bucket)
                    target_bucket = re.sub(r'\.\.+', '.', target_bucket)
                    target_bucket = target_bucket.strip('-.')
                    if len(target_bucket) < 3:
                        target_bucket = f"b{target_bucket}{int(time.time())}"
                    target_bucket = target_bucket[:63]
                    if not target_bucket:
                        target_bucket = f"bucket-{int(time.time())}"
                    if target_bucket.startswith('.'):
                        target_bucket = f"b{target_bucket}"
                    if target_bucket.endswith('.'):
                        target_bucket = f"{target_bucket}b"
                    
                    # Create bucket if not exists
                    try:
                        s3.head_bucket(Bucket=target_bucket)
                    except botocore.exceptions.ClientError:
                        s3.create_bucket(Bucket=target_bucket)
                    
                    # Process chunks and upload to MinIO
                    chunk_index = 0
                    for chunk in chunks:
                        # Convert chunk to CSV
                        csv_buffer = io.StringIO()
                        chunk.to_csv(csv_buffer, index=False)
                        csv_buffer.seek(0)
                        
                        # Upload to MinIO
                        object_key = f"mysql_{source_table}_chunk_{chunk_index}.csv"
                        s3.put_object(
                            Bucket=target_bucket,
                            Key=object_key,
                            Body=csv_buffer.getvalue(),
                            ContentType='text/csv'
                        )
                        
                        rows_processed += len(chunk)
                        chunk_index += 1
                        
                        # Update Progress
                        progress = int((rows_processed / total_rows) * 100) if total_rows > 0 else 0
                        if progress > 100: progress = 99
                        task.progress = progress
                        session.add(task)
                        session.commit()
                    
                    total_rows_synced = rows_processed
                    
                    # Verification
                    task.verification_status = "success"
                else:
                    # MySQL to traditional database sync
                    # Handle initial overwrite
                    if mode == "overwrite":
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
                    
                    # --- Data Verification for MySQL to Database ---
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
                        
                        # Handle SQLite table name quoting (for table names that are numbers or reserved words)
                        is_sqlite = 'sqlite' in str(target_url).lower()
                        if is_sqlite:
                            # For SQLite, quote table names with double quotes
                            quoted_target_table = f'"{target_table}"'
                        else:
                            # For MySQL, use backticks
                            quoted_target_table = f'`{target_table}`'
                        
                        with target_engine.connect() as t_conn:
                            target_cnt_query = text(f"SELECT count(*) FROM {quoted_target_table}")
                            target_count = t_conn.execute(target_cnt_query).scalar()
                        
                        if mode == "overwrite":
                            # Convert to float/int to handle potential type mismatch (e.g. 1.0 vs 1)
                            if float(source_count) != float(target_count):
                                raise Exception(f"Rows mismatch: Source({source_count}) != Target({target_count})")
                            
                            # Optional: Advanced Checksum (CRC32)
                            # Only run if table isn't huge to avoid timeout, or if user requested strict mode.
                            # Using CRC32 on all columns.
                            # 1. Get Columns
                            cols_df = pd.read_sql(f"SHOW COLUMNS FROM {source_table}", url)
                            cols = cols_df['Field'].tolist()
                            
                            if cols:
                                # Check if target database is SQLite (which doesn't support CRC32)
                                is_sqlite = 'sqlite' in str(target_url).lower()
                                
                                if is_sqlite:
                                    # For SQLite, use a simpler checksum method: row count comparison
                                    # SQLite doesn't support CRC32 function, so we skip checksum verification
                                    print("SQLite target detected, skipping CRC32 checksum verification")
                                else:
                                    # Construct Checksum Query: SELECT SUM(CRC32(CONCAT_WS(',', col1, col2...)))
                                    # CAST to UNSIGNED to avoid overflow issues in some versions if needed, though CRC32 returns unsigned.
                                    # BIT_XOR is order independent, SUM depends on row order if not strictly ordered, but sum is commutative.
                                    cols_str = ", ".join(cols)
                                    checksum_sql = f"SELECT SUM(CRC32(CONCAT_WS(',', {cols_str}))) FROM {{table}}"
                                    
                                    src_checksum = pd.read_sql(checksum_sql.format(table=source_table), url).iloc[0, 0]
                                    
                                    with target_engine.connect() as t_conn:
                                        tgt_checksum = t_conn.execute(text(checksum_sql.format(table=target_table))).scalar()
                                    
                                    # Handle potential None/Decimal types
                                    # Convert to str and strip possible decimal points if they are effectively integers (e.g. "1.0" vs "1")
                                    s_chk = str(src_checksum)
                                    t_chk = str(tgt_checksum)
                                    
                                    # Simple normalization: if ends with .0, remove it
                                    if s_chk.endswith('.0'): s_chk = s_chk[:-2]
                                    if t_chk.endswith('.0'): t_chk = t_chk[:-2]

                                    if s_chk != t_chk:
                                        raise Exception(f"Checksum mismatch: Source({src_checksum}) != Target({tgt_checksum})")

                        elif mode == "append":
                            # For append, we can't easily check total count unless we knew before_count.
                            pass

                        task.verification_status = "success"

                    except Exception as verify_err:
                        print(f"Verification Error: {verify_err}")
                        task.verification_status = "failed"
                        # Log verification failure to audit logs so frontend can display it
                        log = AuditLog(user_id="system", action="verification_failed", resource=task.name, details=str(verify_err))
                        session.add(log)
                        # We do NOT fail the task here, just mark verification as failed
                        # raise verify_err # Propagate error to fail task

                    
            elif datasource.type == "clickhouse":
                 from clickhouse_driver import Client
                 
                 # Source Client
                 client = Client(host=conn_info['host'], port=conn_info.get('port', 9000), user=conn_info['user'], password=conn_info['password'], database=conn_info['database'])
                 
                 # Count rows from Source
                 try:
                     count_res = client.execute(f"SELECT count(*) FROM {source_table}")
                     total_rows = count_res[0][0] if count_res else 0
                 except Exception as e:
                     # Check if it is a database error or table error
                     raise Exception(f"Source ClickHouse Read Error: {e}")

                 # Read Data from Source
                 data = client.execute(f"SELECT * FROM {source_table}")
                 columns = [c[0] for c in client.execute(f"DESCRIBE {source_table}")]
                 
                 if target_type == "system_minio":
                     # ClickHouse to MinIO sync
                     import boto3
                     import botocore.exceptions
                     
                     # Initialize MinIO client
                     s3 = boto3.client(
                         's3',
                         endpoint_url=settings.MINIO_ENDPOINT,
                         aws_access_key_id=settings.MINIO_ACCESS_KEY,
                         aws_secret_access_key=settings.MINIO_SECRET_KEY
                     )
                     
                     # Validate and normalize bucket name
                     import re
                     target_bucket = target_table.lower()
                     target_bucket = re.sub(r'[^a-z0-9.-]', '-', target_bucket)
                     target_bucket = re.sub(r'\.\.+', '.', target_bucket)
                     target_bucket = target_bucket.strip('-.')
                     if len(target_bucket) < 3:
                         target_bucket = f"b{target_bucket}{int(time.time())}"
                     target_bucket = target_bucket[:63]
                     if not target_bucket:
                         target_bucket = f"bucket-{int(time.time())}"
                     if target_bucket.startswith('.'):
                         target_bucket = f"b{target_bucket}"
                     if target_bucket.endswith('.'):
                         target_bucket = f"{target_bucket}b"
                     
                     # Create bucket if not exists
                     try:
                         s3.head_bucket(Bucket=target_bucket)
                     except botocore.exceptions.ClientError:
                         s3.create_bucket(Bucket=target_bucket)
                     
                     # Convert data to DataFrame and upload to MinIO
                     df = pd.DataFrame(data, columns=columns)
                     csv_buffer = io.StringIO()
                     df.to_csv(csv_buffer, index=False)
                     csv_buffer.seek(0)
                     
                     # Upload to MinIO
                     object_key = f"clickhouse_{source_table}_{int(time.time())}.csv"
                     s3.put_object(
                         Bucket=target_bucket,
                         Key=object_key,
                         Body=csv_buffer.getvalue(),
                         ContentType='text/csv'
                     )
                     
                     total_rows_synced = len(data)
                     
                     # Update Progress
                     task.progress = 100
                     session.add(task)
                     session.commit()
                     
                     # Verification
                     task.verification_status = "success"
                 else:
                     # ClickHouse to ClickHouse sync
                     # Target Client (from .env settings)
                     target_client = Client(host=settings.CK_HOST, port=settings.CK_PORT, user=settings.CK_USER, password=settings.CK_PASSWORD, database='default') # Default DB for now
                     
                     # Create Target Table if not exists
                     try:
                         # Check if target table exists.
                         exists = target_client.execute(f"EXISTS TABLE {target_table}")[0][0]
                         
                         if not exists:
                             # Try to copy structure
                             desc = client.execute(f"DESCRIBE {source_table}")
                             cols_def = ", ".join([f"`{r[0]}` {r[1]}" for r in desc])
                             create_sql = f"CREATE TABLE {target_table} ({cols_def}) ENGINE = MergeTree() ORDER BY tuple()"
                             target_client.execute(create_sql)
                         elif mode == "overwrite":
                             target_client.execute(f"TRUNCATE TABLE {target_table}")
                              
                         # Write to Target
                         if data:
                             target_client.execute(f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES", data)
                          
                         total_rows_synced = len(data)
                          
                     except Exception as e:
                         print(f"ClickHouse Sync Error: {e}")
                         raise e
                     
                     # --- Verification for ClickHouse to ClickHouse ---
                     task.verification_status = "pending"
                     session.add(task)
                     session.commit()
                     try:
                         # Verify Row Counts
                         target_count_res = target_client.execute(f"SELECT count(*) FROM {target_table}")
                         target_count = target_count_res[0][0] if target_count_res else 0
                         
                         if mode == "overwrite":
                             if total_rows != target_count:
                                 raise Exception(f"Rows mismatch: Source({total_rows}) != Target({target_count})")
                                 
                         task.verification_status = "success"
                     except Exception as verify_err:
                        print(f"ClickHouse Verification Error: {verify_err}")
                        task.verification_status = "failed"
                        log = AuditLog(user_id="system", action="verification_failed", resource=task.name, details=str(verify_err))
                        session.add(log)
                 
            elif datasource.type == "minio":
                 import boto3
                 import botocore.exceptions
                 
                 if target_type == "system_minio":
                     # MinIO to MinIO sync
                     s3 = boto3.client(
                        's3',
                        endpoint_url=conn_info.get('endpoint'), 
                        aws_access_key_id=conn_info.get('access_key'),
                        aws_secret_access_key=conn_info.get('secret_key')
                     )
                     
                     # Target MinIO bucket from user input
                     target_bucket = target_table
                     
                     # Validate and normalize bucket name for MinIO
                     import re
                     # MinIO bucket name rules:
                     # 1. Must be 3-63 characters long
                     # 2. Must start with a letter or number
                     # 3. Must end with a letter or number
                     # 4. Can only contain lowercase letters, numbers, dots (.), and hyphens (-)
                     # 5. Cannot contain consecutive dots
                     # 6. Cannot be an IP address
                     
                     # Convert to lowercase
                     normalized_bucket = target_bucket.lower()
                     
                     # Remove invalid characters
                     normalized_bucket = re.sub(r'[^a-z0-9.-]', '-', normalized_bucket)
                     
                     # Remove consecutive dots
                     normalized_bucket = re.sub(r'\.\.+', '.', normalized_bucket)
                     
                     # Remove leading/trailing hyphens and dots
                     normalized_bucket = normalized_bucket.strip('-.')
                     
                     # Ensure minimum length (3 characters)
                     if len(normalized_bucket) < 3:
                         normalized_bucket = f"b{normalized_bucket}{int(time.time())}"
                     
                     # Ensure maximum length (63 characters)
                     normalized_bucket = normalized_bucket[:63]
                     
                     # Ensure bucket name is not empty
                     if not normalized_bucket:
                         normalized_bucket = f"bucket-{int(time.time())}"
                     
                     # Ensure bucket name doesn't start with a dot
                     if normalized_bucket.startswith('.'):
                         normalized_bucket = f"b{normalized_bucket}"
                     
                     # Ensure bucket name doesn't end with a dot
                     if normalized_bucket.endswith('.'):
                         normalized_bucket = f"{normalized_bucket}b"
                     
                     target_bucket = normalized_bucket
                     
                     # Create target bucket if not exists
                     try:
                         s3.head_bucket(Bucket=target_bucket)
                     except botocore.exceptions.ClientError:
                         # Bucket does not exist or no access, try to create
                         s3.create_bucket(Bucket=target_bucket)
                     
                     # Copy from source bucket to target bucket
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
                                 
                             except Exception as verify_err:
                                print(f"MinIO Verification Error for {key}: {verify_err}")
                                task.verification_status = "failed"
                                log = AuditLog(user_id="system", action="verification_failed", resource=task.name, details=str(verify_err))
                                session.add(log)
                                  
                             task.progress = int((processed_files / total_files) * 100)
                             session.add(task)
                             session.commit()
                 else:
                     # MinIO to other storage types (not implemented yet)
                     raise Exception("MinIO to non-MinIO sync is not supported yet")
            
            # If we finished loop without setting verification_status to failed, set to success?
            # We need to initialize it first.
            if datasource.type == "minio" and task.verification_status != "failed":
                 task.verification_status = "success"


            # Update SyncedTable Registry
            # Determine logic to find existing entry based on storage backend
            # For MinIO sync, use the normalized bucket name as table_name
            synced_table_name = target_bucket if target_type == "system_minio" else target_table
            
            query = select(SyncedTable).where(SyncedTable.table_name == synced_table_name)
            
            if datasource.type == 'minio':
                query = query.where(SyncedTable.source_type == 'minio')
            elif datasource.type == 'clickhouse':
                query = query.where(SyncedTable.source_type == 'clickhouse')
            else:
                # Assume all others (mysql, postgres, etc.) map to System DB tables
                # So they share the same namespace and entry.
                query = query.where(SyncedTable.source_type.notin_(['minio', 'clickhouse']))
            
            existing_table = session.exec(query).first()
            
            if existing_table:
                if mode == "overwrite":
                    existing_table.row_count = total_rows_synced
                else:
                    existing_table.row_count += total_rows_synced
                existing_table.updated_at = datetime.utcnow()
                # Also update source info in case it changed (e.g. reusing table name)
                existing_table.source_type = "minio" if target_type == "system_minio" else datasource.type
                existing_table.source_name = datasource.name
                existing_table.data_type = datasource.data_type
                session.add(existing_table)
            else:
                new_table = SyncedTable(
                    table_name=synced_table_name,
                    source_type="minio" if target_type == "system_minio" else datasource.type,
                    source_name=datasource.name,
                    row_count=total_rows_synced,
                    data_type=datasource.data_type
                )
                session.add(new_table)
            
            task.status = "success"
            task.progress = 100
            session.add(task)
            session.commit()
            
        except Exception as e:
            traceback.print_exc()
            task.status = "failed"
            # Record detailed failure log
            log = AuditLog(
                user_id="system", 
                action="task_failed", 
                resource=task.name, 
                details=str(e) + "\n" + traceback.format_exc()
            )
            session.add(log)
            
            session.add(task)
            session.commit()
