import subprocess
import json
import os
import sys
from backend.app.models.task import DataTask

def submit_spark_job(task: DataTask):
    # 1. Prepare Config File
    config_dir = "temp_configs"
    os.makedirs(config_dir, exist_ok=True)
    config_path = os.path.abspath(f"{config_dir}/task_{task.id}.json")
    
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
        return False, e.stderr
