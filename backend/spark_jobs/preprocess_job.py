import sys
import json
import argparse
import os
from datetime import datetime
import traceback

# Import Operators
try:
    from pyspark.sql import SparkSession
    from backend.operators.cleaning import dedup, filter_rows
    from backend.operators.missing import fill_na, drop_na
    from backend.operators.exploration import explore
    from backend.operators.outliers import handle_outliers
    from backend.operators.transformation import standardize, rename_columns
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

import pandas as pd
from sqlalchemy import create_engine, text

def get_spark_session(app_name: str):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def register_asset(config, table_name, source_type, row_count=0):
    """
    Register the output table in SyncedTable registry via System DB.
    """
    system_db_url = config.get("system_db_url")
    if not system_db_url:
        print("Warning: system_db_url not provided, skipping registration.")
        return

    try:
        engine = create_engine(system_db_url)
        with engine.connect() as conn:
            # Check if exists
            # Using raw SQL to avoid SQLModel dependency here if possible, 
            # but we know the schema: syncedtable (table_name, source_type, source_name, row_count, ...)
            
            # Simple upsert logic
            # 1. Check
            check_sql = text("SELECT id FROM syncedtable WHERE table_name = :name AND source_type = :type")
            existing = conn.execute(check_sql, {"name": table_name, "type": source_type}).fetchone()
            
            now = datetime.utcnow()
            
            if existing:
                update_sql = text("""
                    UPDATE syncedtable 
                    SET row_count = :rows, updated_at = :now 
                    WHERE id = :id
                """)
                conn.execute(update_sql, {"rows": row_count, "now": now, "id": existing[0]})
                print(f"Updated registry for {table_name}")
            else:
                insert_sql = text("""
                    INSERT INTO syncedtable (table_name, source_type, source_name, row_count, created_at, updated_at)
                    VALUES (:name, :type, :src, :rows, :now, :now)
                """)
                conn.execute(insert_sql, {
                    "name": table_name, 
                    "type": source_type, 
                    "src": config.get("job_name", "preprocess_job"), 
                    "rows": row_count, 
                    "now": now
                })
                print(f"Registered new asset {table_name}")
            conn.commit()
    except Exception as e:
        print(f"Error registering asset: {e}")

def run_pandas_job(config):
    print("Running in Pandas Mode.")
    
    # 1. Read Data
    source = config["source"]
    df = None
    
    try:
        if source["type"] == "csv":
            df = pd.read_csv(source["path"])
        elif source["type"] == "parquet":
            df = pd.read_parquet(source["path"])
        elif source["type"] == "mysql":
             # Use source_connection if available
             src_conf = config.get("source_connection")
             if src_conf:
                 url = f"mysql+pymysql://{src_conf['user']}:{src_conf['password']}@{src_conf['host']}:{src_conf['port']}/{src_conf['database']}"
             else:
                 url = source.get("url") or config.get("system_db_url")
                 
             query = source.get("query", f"SELECT * FROM {source.get('table')}")
             engine = create_engine(url)
             df = pd.read_sql(query, engine)
        elif source["type"] == "clickhouse":
             from clickhouse_driver import Client
             
             # Use source_connection if available
             src_conf = config.get("source_connection")
             sys_ck = config.get("clickhouse", {})
             
             if src_conf:
                 host = src_conf.get('host')
                 port = src_conf.get('port')
                 user = src_conf.get('user')
                 password = src_conf.get('password')
                 database = src_conf.get('database', 'default')
             else:
                 host = sys_ck.get('host')
                 port = sys_ck.get('port')
                 user = sys_ck.get('user')
                 password = sys_ck.get('password')
                 database = 'default'

             client = Client(host=host, port=port, user=user, password=password, database=database)
             query = source.get("query", f"SELECT * FROM {source.get('table')}")
             data, columns = client.execute(query, with_column_types=True)
             df = pd.DataFrame(data, columns=[c[0] for c in columns])
        else:
            raise ValueError(f"Unsupported source type: {source['type']}")
    except Exception as e:
         print(f"Error reading source: {e}")
         raise

    print(f"Initial rows: {len(df)}")

    # 2. Apply Operators
    operators = config.get("operators", [])
    for op in operators:
        op_type = op["type"]
        print(f"Applying {op_type}...")
        
        if op_type == "dedup":
            cols = op.get("columns")
            if cols:
                df = df.drop_duplicates(subset=cols)
            else:
                df = df.drop_duplicates()
                
        elif op_type == "filter":
            df = df.query(op["condition"])
            
        elif op_type == "fill_na":
            val = op.get("value")
            cols = op.get("columns")
            if cols:
                df[cols] = df[cols].fillna(val)
            else:
                df = df.fillna(val)
                
        elif op_type == "drop_na":
            cols = op.get("columns")
            if cols:
                df = df.dropna(subset=cols)
            else:
                df = df.dropna()
                
        elif op_type == "explore":
            # Just print stats for now, or save to a file
            desc = df.describe().to_json()
            print("Exploration Report:", desc)
            # Optionally write to report file
            
        elif op_type == "outliers":
            # IQR Method
            cols = op.get("columns")
            if not cols:
                cols = df.select_dtypes(include=['number']).columns.tolist()
            
            for c in cols:
                Q1 = df[c].quantile(0.25)
                Q3 = df[c].quantile(0.75)
                IQR = Q3 - Q1
                df = df[~((df[c] < (Q1 - 1.5 * IQR)) | (df[c] > (Q3 + 1.5 * IQR)))]
                
        elif op_type == "standardize":
            cols = op.get("columns")
            if not cols:
                cols = df.select_dtypes(include=['number']).columns.tolist()
            for c in cols:
                if df[c].std() != 0:
                    df[c] = (df[c] - df[c].mean()) / df[c].std()
                    
        elif op_type == "rename":
            mapping = op.get("mapping", {})
            df = df.rename(columns=mapping)

    print(f"Final rows: {len(df)}")
    
    # 3. Write Data
    target = config["target"]
    target_type = target.get("type", "csv")
    
    # Support "system_mysql" and "system_clickhouse" aliases
    if target_type == "system_mysql":
        target_type = "mysql"
        target["url"] = config.get("system_db_url")
    elif target_type == "system_clickhouse":
        target_type = "clickhouse"
        
    if target_type == "mysql" or target_type == "jdbc":
         try:
            url = target.get("url")
            # If URL is still missing for mysql (e.g. system_mysql was used but url not populated correctly)
            if not url and target_type == "mysql":
                url = config.get("system_db_url")
            
            table = target.get("table")
            mode = target.get("mode", "append")
            if_exists = "replace" if mode == "overwrite" else "append"
                
            engine = create_engine(url)
            df.to_sql(table, engine, if_exists=if_exists, index=False)
            print(f"Written to MySQL table {table}")
            
            # Register
            register_asset(config, table, "mysql", len(df))
            
         except Exception as e:
             print(f"Error writing to JDBC: {e}")
             raise

    elif target_type == "clickhouse":
        try:
             from clickhouse_driver import Client
             ck_conf = config.get("clickhouse", {})
             
             # Use system ClickHouse config
             host = ck_conf.get('host')
             port = ck_conf.get('port')
             user = ck_conf.get('user')
             password = ck_conf.get('password')
             
             # If target URL is provided (jdbc style), try to parse? 
             # For Pandas mode with system_clickhouse, we rely on the injected 'clickhouse' config.
             
             client = Client(host=host, port=port, user=user, password=password)
             
             table = target.get("table")
             mode = target.get("mode", "append")
             
             # Create table if not exists (simple inference)
             # This is tricky for ClickHouse as we need types.
             # For Pandas, we can map dtypes to CH types.
             # MVP: Assume table exists OR create simple one.
             # Let's try to create if not exists with generic String/Float
             
             if mode == "overwrite":
                 client.execute(f"DROP TABLE IF EXISTS {table}")
                 
             # Check existence
             exists = client.execute(f"EXISTS TABLE {table}")[0][0]
             
             if not exists:
                 # Infer schema
                 cols_def = []
                 for name, dtype in df.dtypes.items():
                     if "int" in str(dtype): ch_type = "Int64"
                     elif "float" in str(dtype): ch_type = "Float64"
                     else: ch_type = "String"
                     cols_def.append(f"`{name}` {ch_type}")
                 
                 create_sql = f"CREATE TABLE {table} ({', '.join(cols_def)}) ENGINE = MergeTree() ORDER BY tuple()"
                 client.execute(create_sql)
             
             # Insert
             # For ClickHouse, ensure data types match strictly or convert
             # Pandas object types might need conversion to String
             
             # Convert object columns to string to avoid potential issues
             # or rely on driver conversion.
             
             client.insert_dataframe(f"INSERT INTO {table} VALUES", df)
             print(f"Written to ClickHouse table {table}")
             
             # Register
             register_asset(config, table, "clickhouse", len(df))
             
        except Exception as e:
            print(f"Error writing to ClickHouse: {e}")
            raise

    else:
        target_path = target["path"]
        os.makedirs(target_path, exist_ok=True)
        output_file = os.path.join(target_path, "part-00000.csv")
        
        if target_type == "parquet":
            df.to_parquet(output_file)
        else:
            df.to_csv(output_file, index=False)
            
        print(f"Written to {output_file}")
        # Local file registration could be added if we tracked file assets by name

def run_job(config_path: str):
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    try:
        if not SPARK_AVAILABLE:
            raise Exception("PySpark module not found")
            
        spark = get_spark_session(config.get("job_name", "PreprocessJob"))
        
        # 1. Read Data
        source = config["source"]
        df = None
        if source["type"] == "csv":
            df = spark.read.option("header", "true").csv(source["path"])
        elif source["type"] == "parquet":
            df = spark.read.parquet(source["path"])
        elif source["type"] == "clickhouse" or source["type"] == "jdbc" or source["type"] == "mysql":
             # Spark JDBC
             url = source.get("url")
             dbtable = source.get("table")
             user = "default"
             password = ""
             
             src_conf = config.get("source_connection")
             
             if source["type"] == "clickhouse":
                 if not url:
                     if src_conf:
                         host = src_conf.get('host')
                         port = src_conf.get('port')
                         database = src_conf.get('database', 'default')
                         url = f"jdbc:clickhouse://{host}:{port}/{database}"
                         user = src_conf.get('user', 'default')
                         password = src_conf.get('password', '')
                     else:
                         ck_conf = config.get("clickhouse", {})
                         url = f"jdbc:clickhouse://{ck_conf.get('host')}:{ck_conf.get('port')}"
                         user = ck_conf.get('user', 'default')
                         password = ck_conf.get('password', '')
                         
             elif source["type"] == "mysql":
                  if not url:
                      if src_conf:
                          url = f"jdbc:mysql://{src_conf['host']}:{src_conf['port']}/{src_conf['database']}"
                          user = src_conf.get('user')
                          password = src_conf.get('password')
                      else:
                          # Fallback (System DB)
                          url = config.get("system_db_url").replace("mysql+pymysql://", "jdbc:mysql://")
                          # Extract user/pass from url or use default? 
                          # Ideally parse system_db_url but for now let's rely on src_conf mostly.
                          
             df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", dbtable) \
                .option("user", user) \
                .option("password", password) \
                .load()
        
        # 2. Apply Operators
        operators = config.get("operators", [])
        for op in operators:
            op_type = op["type"]
            if op_type == "dedup":
                df = dedup(df, op.get("columns"))
            elif op_type == "filter":
                df = filter_rows(df, op["condition"])
            elif op_type == "fill_na":
                df = fill_na(df, value=op.get("value"), columns=op.get("columns"))
            elif op_type == "drop_na":
                df = drop_na(df, columns=op.get("columns"))
            elif op_type == "explore":
                print(explore(df))
            elif op_type == "outliers":
                df = handle_outliers(df, method="iqr", columns=op.get("columns"))
            elif op_type == "standardize":
                df = standardize(df, columns=op.get("columns"))
            elif op_type == "rename":
                df = rename_columns(df, op.get("mapping", {}))
        
        # 3. Write Data
        target = config["target"]
        raw_target_type = target.get("type", "csv")
        target_type = raw_target_type
        
        # Alias handling
        if raw_target_type in ("system_mysql", "system_clickhouse"):
            target_type = "jdbc"
        
        row_count = df.count()
        
        if target_type == "jdbc" or target_type == "mysql" or target_type == "clickhouse":
             url = target.get("url")
             is_clickhouse = (raw_target_type in ("system_clickhouse", "clickhouse")) or (url and "clickhouse" in url)
             is_mysql = (raw_target_type in ("system_mysql", "mysql")) or (url and "mysql" in url)
             
             # Fallback logic for System DBs if URL is missing
             if not url:
                 if is_clickhouse:
                     ck_conf = config.get("clickhouse", {})
                     host = ck_conf.get('host')
                     port = ck_conf.get('port')
                     database = ck_conf.get("database") or "default"
                     url = f"jdbc:clickhouse://{host}:{port}/{database}"
                 elif is_mysql:
                      sys_url = config.get("system_db_url")
                      if sys_url:
                          try:
                              from sqlalchemy.engine.url import make_url
                              parsed = make_url(sys_url)
                              host = parsed.host
                              port = parsed.port or 3306
                              database = parsed.database
                              url = f"jdbc:mysql://{host}:{port}/{database}"
                          except Exception:
                              url = None

             jdbc_user = None
             jdbc_password = None
             if is_clickhouse:
                 ck_conf = config.get("clickhouse", {})
                 jdbc_user = ck_conf.get("user", "default")
                 jdbc_password = ck_conf.get("password", "")
             elif is_mysql:
                 sys_url = config.get("system_db_url")
                 if sys_url:
                     try:
                         from sqlalchemy.engine.url import make_url
                         parsed = make_url(sys_url)
                         jdbc_user = parsed.username
                         jdbc_password = parsed.password or ""
                     except Exception:
                         jdbc_user = None
                         jdbc_password = None

             write_mode = target.get("mode", "overwrite")
             
             writer = df.write \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", target.get("table")) \
                .option("user", jdbc_user or "") \
                .option("password", jdbc_password or "") \
                .mode(write_mode)
            
             # Add driver option if needed? Usually Spark detects from URL or classpath.
             # For ClickHouse: ru.yandex.clickhouse.ClickHouseDriver or com.clickhouse.jdbc.ClickHouseDriver
             # For MySQL: com.mysql.cj.jdbc.Driver
             
             writer.save()
                
             # Register
             # Map target_type to simple string for registry
             reg_type = "clickhouse" if "clickhouse" in url else "mysql"
             register_asset(config, target.get("table"), reg_type, row_count)
             
        else:
             write_mode = target.get("mode", "overwrite")
             if target_type == "parquet":
                df.write.mode(write_mode).parquet(target["path"])
             else:
                df.write.mode(write_mode).option("header", "true").csv(target["path"])
        
        spark.stop()
        
    except Exception as e:
        print(f"Spark execution failed/unavailable: {e}")
        traceback.print_exc()
        print("Falling back to Pandas execution...")
        try:
            run_pandas_job(config)
        except Exception as pandas_err:
            print(f"Pandas execution also failed: {pandas_err}")
            traceback.print_exc()
            raise pandas_err # Raise the final error to be caught by wrapper

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to job config JSON")
    args = parser.parse_args()
    
    run_job(args.config)
