# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer - Invoice Analytics
# MAGIC 
# MAGIC **Purpose**: Create business-ready analytics tables from silver data
# MAGIC 
# MAGIC **Tables Created**:
# MAGIC - `invoice_summary`: Aggregated invoice metrics
# MAGIC - `data_quality_metrics`: Pipeline health monitoring
# MAGIC - `processing_stats`: ETL execution statistics

# COMMAND ----------

# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define tables
SILVER_TABLE = f"{catalog_name}.silver.invoices_clean"
GOLD_SUMMARY_TABLE = f"{catalog_name}.gold.invoice_summary"
GOLD_QUALITY_TABLE = f"{catalog_name}.gold.data_quality_metrics"

print(f"Source: {SILVER_TABLE}")
print(f"Gold Tables: {GOLD_SUMMARY_TABLE}, {GOLD_QUALITY_TABLE}")

# COMMAND ----------

# Read silver data
df_silver = spark.table(SILVER_TABLE)

print(f"Silver records: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Invoice Summary Table

# COMMAND ----------

# Aggregate invoice metrics
df_invoice_summary = (df_silver
    .filter(col("_data_quality_score") >= 0.5)  # Only use quality records
    .groupBy("_environment")
    .agg(
        count("*").alias("total_invoices"),
        countDistinct("row_hash").alias("unique_invoices"),
        max("_load_timestamp").alias("latest_load_timestamp"),
        min("_load_timestamp").alias("earliest_load_timestamp"),
        avg("_data_quality_score").alias("avg_quality_score")
    )
    .withColumn("processing_date", current_date())
    .withColumn("processing_timestamp", current_timestamp())
)

# Write to Gold table
(df_invoice_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_SUMMARY_TABLE)
)

print(f"✓ Created {GOLD_SUMMARY_TABLE}")
display(df_invoice_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Quality Metrics Table

# COMMAND ----------

# Calculate data quality metrics
df_quality_metrics = (df_silver
    .groupBy("_environment", "_data_quality_score")
    .agg(
        count("*").alias("record_count")
    )
    .withColumn("total_records", sum("record_count").over(Window.partitionBy("_environment")))
    .withColumn("percentage", round(col("record_count") / col("total_records") * 100, 2))
    .withColumn("metric_timestamp", current_timestamp())
    .withColumn("metric_date", current_date())
)

# Write to Gold table
(df_quality_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_QUALITY_TABLE)
)

print(f"✓ Created {GOLD_QUALITY_TABLE}")
display(df_quality_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics Summary Report

# COMMAND ----------

print("="*80)
print("GOLD LAYER ANALYTICS SUMMARY")
print("="*80)

# Summary metrics
summary_df = spark.table(GOLD_SUMMARY_TABLE)
quality_df = spark.table(GOLD_QUALITY_TABLE)

print("\n📊 Invoice Summary:")
display(summary_df)

print("\n📈 Data Quality Distribution:")
display(quality_df.orderBy("_data_quality_score"))

# Calculate pipeline efficiency
bronze_count = spark.table(f"{catalog_name}.bronze.invoices_raw").count()
silver_count = spark.table(f"{catalog_name}.silver.invoices_clean").count()
gold_count = summary_df.select(sum("total_invoices")).collect()[0][0]

print(f"\n🔄 Pipeline Flow:")
print(f"  Bronze → Silver: {bronze_count:,} → {silver_count:,} ({silver_count/bronze_count*100:.2f}% retention)")
print(f"  Silver → Gold:   {silver_count:,} → {gold_count:,} ({gold_count/silver_count*100:.2f}% retention)")

print(f"\n✓ Gold layer processing complete!")

