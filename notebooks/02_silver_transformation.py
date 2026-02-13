# COMMAND ----------
# SILVER LAYER: Data Cleansing & Evolution
# Implementing Data Quality Expectations and SCD logic
# COMMAND ----------

import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="cleaned_sales_orders",
    comment="Cleansed sales records with applied data quality constraints"
)
@dlt.expect_or_drop("valid_amount", "total_amount > 0")
@dlt.expect_or_fail("valid_id", "order_id IS NOT NULL")
def cleaned_sales_orders():
    return (
        dlt.read_stream("raw_sales_data")
        .filter(col("_automl_status") == "verified")
        .select(
            "order_id",
            "customer_id",
            "total_amount",
            "transaction_date",
            "ingestion_timestamp"
        )
        .dropDuplicates(["order_id"])
    )
