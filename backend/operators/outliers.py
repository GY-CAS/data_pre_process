from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, FloatType, LongType

def handle_outliers(df: DataFrame, method: str = "iqr", columns: list = None) -> DataFrame:
    """
    Handle outliers. Currently supports IQR filtering.
    """
    if not columns:
        # Auto-detect numeric columns
        columns = [f.name for f in df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, FloatType, LongType))]
    
    if method == "iqr":
        for col_name in columns:
            # approxQuantile is efficient for Spark
            quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.05) # 0.05 relative error
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Filter rows keeping only valid ones
            df = df.filter((F.col(col_name) >= lower_bound) & (F.col(col_name) <= upper_bound))
            
    return df
