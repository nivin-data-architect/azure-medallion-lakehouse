
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Optional

def apply_salting(
    df: DataFrame, 
    skew_column: str, 
    salt_bins: int = 10, 
    alias_prefix: Optional[str] = None
) -> DataFrame:
    """
    Optimizes Spark joins by mitigating data skew through a 'Salting' technique.
    
    This function adds a random integer column (salt) to redistribute skewed keys 
    across multiple partitions, preventing 'straggler' tasks.

    Args:
        df (DataFrame): The input Spark DataFrame containing skewed data.
        skew_column (str): The column name causing the data skew (e.g., 'customer_id').
        salt_bins (int): The number of partitions to spread the skew across. Default is 10.
        alias_prefix (str, optional): A prefix for the salted column name to avoid collisions.

    Returns:
        DataFrame: A new DataFrame with the added 'salt' and 'salted_key' columns.
    """
    
    # Validation: Ensure the skew column exists in the DataFrame
    if skew_column not in df.columns:
        raise ValueError(f"Column '{skew_column}' not found in input DataFrame.")

    salt_col_name = f"{alias_prefix}_salt" if alias_prefix else "salt"
    salted_key_name = f"{alias_prefix}_salted_key" if alias_prefix else "salted_key"

    return (
        df.withColumn(salt_col_name, (F.rand() * salt_bins).cast("int"))
          .withColumn(salted_key_name, F.concat(F.col(skew_column), F.lit("_"), F.col(salt_col_name)))
    )

def replicate_for_salt(
    df: DataFrame, 
    join_column: str, 
    salt_bins: int = 10
) -> DataFrame:
    """
    Prepares the smaller (non-skewed) table for a salted join by replicating rows.
    """
    # Create a list of all possible salt values [0, 1, 2... salt_bins-1]
    salt_values = F.array([F.lit(i) for i in range(salt_bins)])
    
    return (
        df.withColumn("salt_array", salt_values)
          .withColumn("salt", F.explode(F.col("salt_array")))
          .withColumn("salted_key", F.concat(F.col(join_column), F.lit("_"), F.col("salt")))
          .drop("salt_array", "salt")
    )