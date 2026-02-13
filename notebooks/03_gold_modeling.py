# COMMAND ----------
# GOLD LAYER: Business Readiness & Performance Optimization
# Optimized for Power BI consumption using Liquid Clustering
# COMMAND ----------

import dlt
from pyspark.sql.functions import sum, count, col

@dlt.table(
    name="fact_monthly_sales_summary",
    comment="Gold layer aggregate table for Executive Dashboards",
    table_properties={"delta.enableLiquidClustering": "true"} # High-end performance feature
)
def fact_monthly_sales_summary():
    return (
        dlt.read("cleaned_sales_orders")
        .groupBy("customer_id", "transaction_date")
        .agg(
            sum("total_amount").alias("monthly_revenue"),
            count("order_id").alias("order_count")
        )
    )

# Note: In a production environment, we apply OPTIMIZE and VACUUM 
# as part of the Databricks Workflow maintenance task.
