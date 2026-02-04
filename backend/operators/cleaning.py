from pyspark.sql import DataFrame

def dedup(df: DataFrame, columns: list = None) -> DataFrame:
    """
    Remove duplicate rows.
    :param df: Input DataFrame
    :param columns: List of columns to consider for identifying duplicates. If None, considers all columns.
    :return: DataFrame with duplicates removed
    """
    if columns:
        return df.dropDuplicates(columns)
    return df.dropDuplicates()

def filter_rows(df: DataFrame, condition: str) -> DataFrame:
    """
    Filter rows based on a SQL-like condition.
    :param df: Input DataFrame
    :param condition: SQL condition string (e.g., "age > 18")
    :return: Filtered DataFrame
    """
    return df.filter(condition)
