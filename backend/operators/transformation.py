from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, FloatType, LongType

def standardize(df: DataFrame, columns: list = None) -> DataFrame:
    """
    Standardize numeric columns (Z-Score normalization).
    """
    if not columns:
        columns = [f.name for f in df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, FloatType, LongType))]
    
    if not columns:
        return df

    # simple optimization: calculate stats in one pass
    exprs = []
    for c in columns:
        exprs.append(F.mean(c).alias(c + '_mean'))
        exprs.append(F.stddev(c).alias(c + '_std'))
        
    stats = df.select(*exprs).collect()[0]
    
    for c in columns:
        mean = stats[c + '_mean']
        std = stats[c + '_std']
        if std is not None and std != 0:
            df = df.withColumn(c, (F.col(c) - mean) / std)
            
    return df

def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """
    Rename columns based on mapping {old: new}
    """
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df
