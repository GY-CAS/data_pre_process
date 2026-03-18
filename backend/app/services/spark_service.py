import subprocess
import json
import os
import sys
from sqlmodel import Session
from app.models.task import DataTask
from app.models.datasource import DataSource
from app.core.db import get_engine
from app.core.config import settings

def submit_spark_job(task: DataTask):
    config_dir = "temp_configs"
    os.makedirs(config_dir, exist_ok=True)
    config_path = os.path.abspath(f"{config_dir}/task_{task.id}.json")
    
    try:
        job_config = json.loads(task.config)
        job_config['system_db_url'] = settings.SYSTEM_DB_URL
        job_config['clickhouse'] = {
            'host': settings.CK_HOST,
            'port': settings.CK_PORT,
            'user': settings.CK_USER,
            'password': settings.CK_PASSWORD
        }
        job_config['task_id'] = task.id
        
        if 'source_id' in job_config:
             with Session(get_engine()) as session:
                 ds = session.get(DataSource, job_config['source_id'])
                 if ds:
                     try:
                         conn_info = json.loads(ds.connection_info)
                         job_config['source_connection'] = conn_info
                         if 'source' in job_config:
                             job_config['source']['type'] = ds.type 
                     except Exception as e:
                         print(f"Error resolving data source: {e}")

        with open(config_path, 'w') as f:
            json.dump(job_config, f, indent=2)
    except Exception as e:
        print(f"Error parsing task config: {e}")
        with open(config_path, 'w') as f:
            f.write(task.config)
    
    script_path = os.path.abspath("backend/spark_jobs/preprocess_job.py")
    
    cmd = [
        sys.executable,
        script_path,
        "--config", config_path
    ]
    
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
