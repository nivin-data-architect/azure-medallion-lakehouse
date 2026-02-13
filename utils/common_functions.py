from pyspark.sql import functions as F

def apply_salting(df, salt_bins=10):
    """
    Solves Data Skew by adding a random salt column to the DataFrame.
    """
    return df.withColumn("salt", (F.rand() * salt_bins).cast("int"))

def add_metadata(df):
    """
    Standardizes metadata columns across the Medallion layers.
    """
    return df.withColumn("load_timestamp", F.current_timestamp()) \
             .withColumn("source_system", F.lit("ERP_Azure_SQL"))

def log_pipeline_status(pipeline_name, status):
    """
    Utility for custom logging into a centralized Delta table.
    """
    print(f"Pipeline {pipeline_name} status: {status}")
