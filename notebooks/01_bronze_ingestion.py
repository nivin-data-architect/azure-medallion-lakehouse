# COMMAND ----------
# BRONZE LAYER: Raw Ingestion using Cloud Files (Autoloader)
# Optimized for high-volume landing zones in ADLS Gen2
# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

@dlt.table(
    name="raw_sales_data",
    comment="Raw sales data ingested from ADLS Gen2 landing zone"
)
def raw_sales_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/mnt/telemetry/checkpoints/sales_raw")
        .load("/mnt/telemetry/landing/sales/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
