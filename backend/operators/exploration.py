from pyspark.sql import DataFrame
import json

def explore(df: DataFrame) -> str:
    """
    Perform exploratory analysis and return a JSON string report.
    """
    count = df.count()
    columns = df.columns
    # summary() computes count, mean, stddev, min, 25%, 50%, 75%, max
    summary_df = df.summary()
    summary_json = summary_df.toJSON().collect()
    
    profile = {
        "total_rows": count,
        "columns": columns,
        "summary": [json.loads(s) for s in summary_json]
    }
    return json.dumps(profile)
