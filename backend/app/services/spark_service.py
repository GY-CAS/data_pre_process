import subprocess
import json
import os
import sys
from sqlmodel import Session
from backend.app.models.task import DataTask
from backend.app.models.datasource import DataSource
from backend.app.core.db import engine
from backend.app.core.config import settings

def submit_spark_job(task: DataTask):
    # 1. Prepare Config File
    config_dir = "temp_configs"
    os.makedirs(config_dir, exist_ok=True)
    config_path = os.path.abspath(f"{config_dir}/task_{task.id}.json")
    
    # Inject System Settings into Job Config
    try:
        job_config = json.loads(task.config)
        job_config['system_db_url'] = settings.SYSTEM_DB_URL
        job_config['clickhouse'] = {
            'host': settings.CK_HOST,
            'port': settings.CK_PORT,
            'user': settings.CK_USER,
            'password': settings.CK_PASSWORD
        }
        # Add task_id for tracking if needed
        job_config['task_id'] = task.id
        
        # Resolve Source Connection if source_id is present
        if 'source_id' in job_config:
             with Session(engine) as session:
                 ds = session.get(DataSource, job_config['source_id'])
                 if ds:
                     try:
                         conn_info = json.loads(ds.connection_info)
                         job_config['source_connection'] = conn_info
                         # Ensure source type matches
                         if 'source' in job_config:
                             job_config['source']['type'] = ds.type 
                     except Exception as e:
                         print(f"Error resolving data source: {e}")

        with open(config_path, 'w') as f:
            json.dump(job_config, f, indent=2)
    except Exception as e:
        print(f"Error parsing task config: {e}")
        # Fallback to raw config if parse fails (shouldn't happen)
        with open(config_path, 'w') as f:
            f.write(task.config)
    
    # 2. Determine Script Path
    # Assuming we run from project root
    script_path = os.path.abspath("backend/spark_jobs/preprocess_job.py")
    
    # 3. Construct Command
    # We use sys.executable to ensure we use the same python environment
    # In production this would be 'spark-submit'
    cmd = [
        sys.executable,
        script_path,
        "--config", config_path
    ]
    
    # 4. Run Command
    # We set PYTHONPATH to include current directory so backend modules can be imported
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
    
    print(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            env=env,
            check=True
        )
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        print("Error executing Spark job")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        combined = ""
        if e.stderr:
            combined += e.stderr
        if e.stdout:
            combined += ("\n" if combined else "") + e.stdout
        return False, combined
