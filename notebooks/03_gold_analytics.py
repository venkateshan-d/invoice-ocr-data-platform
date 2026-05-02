# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# DBTITLE 1,Gold Layer - Invoice Analytics
# MAGIC %md
# MAGIC # Gold Layer - Analytics
# MAGIC
# MAGIC Creates business-ready analytics tables.

# COMMAND ----------

# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Define Gold Tables
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# Define tables
SILVER_TABLE = f"{catalog_name}.silver.invoices_clean"
GOLD_SUMMARY_TABLE = f"{catalog_name}.gold.invoice_summary"
GOLD_QUALITY_TABLE = f"{catalog_name}.gold.data_quality_metrics"
GOLD_VENDOR_TABLE = f"{catalog_name}.gold.vendor_analytics"

print(f"Source: {SILVER_TABLE}")
print(f"Gold Tables:")
print(f"  - {GOLD_SUMMARY_TABLE}")
print(f"  - {GOLD_QUALITY_TABLE}")
print(f"  - {GOLD_VENDOR_TABLE}")

# COMMAND ----------

# Read silver data
df_silver = spark.table(SILVER_TABLE)

print(f"Silver records: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Invoice Summary Table

# COMMAND ----------

# DBTITLE 1,Create Invoice Summary Table
# Aggregate invoice metrics
df_invoice_summary = (df_silver
    .filter(col("_data_quality_score") >= 0.6)  # Use reasonably complete invoices
    .groupBy(
        date_trunc("month", col("invoice_date_parsed")).alias("invoice_month"),
        col("vendor_name_clean").alias("vendor"),
        col("_environment")
    )
    .agg(
        count("*").alias("total_invoices"),
        countDistinct("invoice_number").alias("unique_invoices"),
        sum("total_amount_parsed").alias("total_amount_sum"),
        avg("total_amount_parsed").alias("avg_invoice_amount"),
        min("total_amount_parsed").alias("min_invoice_amount"),
        max("total_amount_parsed").alias("max_invoice_amount"),
        avg("_data_quality_score").alias("avg_quality_score")
    )
    .withColumn("processing_date", current_date())
    .withColumn("processing_timestamp", current_timestamp())
    .orderBy("invoice_month", "vendor")
)

# Write to Gold table
(df_invoice_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_SUMMARY_TABLE)
)

print(f"✓ Created {GOLD_SUMMARY_TABLE}")
print(f"\nSample Invoice Summary:")
display(df_invoice_summary.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Quality Metrics Table

# COMMAND ----------

# DBTITLE 1,Create Vendor Analytics Table
# Vendor-level analytics
df_vendor_analytics = (df_silver
    .filter(col("vendor_name_clean").isNotNull())
    .groupBy(col("vendor_name_clean").alias("vendor_name"))
    .agg(
        count("*").alias("total_invoices"),
        sum("total_amount_parsed").alias("total_revenue"),
        avg("total_amount_parsed").alias("avg_invoice_value"),
        min("invoice_date_parsed").alias("first_invoice_date"),
        max("invoice_date_parsed").alias("last_invoice_date"),
        avg("_data_quality_score").alias("avg_data_quality")
    )
    .withColumn("days_active", datediff(col("last_invoice_date"), col("first_invoice_date")))
    .withColumn("processing_timestamp", current_timestamp())
    .orderBy(desc("total_revenue"))
)

# Write to Gold table
(df_vendor_analytics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_VENDOR_TABLE)
)

print(f"✓ Created {GOLD_VENDOR_TABLE}")
print(f"\nTop Vendors by Revenue:")
display(df_vendor_analytics.limit(10))

# COMMAND ----------

# DBTITLE 1,Create Data Quality Metrics Table
# Calculate data quality metrics
df_quality_metrics = (df_silver
    .groupBy(
        col("_environment"),
        when(col("_data_quality_score") >= 0.8, "High (0.8-1.0)")
        .when(col("_data_quality_score") >= 0.6, "Medium (0.6-0.8)")
        .when(col("_data_quality_score") >= 0.4, "Low (0.4-0.6)")
        .otherwise("Very Low (<0.4)").alias("quality_tier")
    )
    .agg(
        count("*").alias("record_count"),
        avg("fields_extracted").alias("avg_fields_extracted"),
        sum(when(col("invoice_number").isNotNull(), 1).otherwise(0)).alias("invoice_number_extracted"),
        sum(when(col("invoice_date_parsed").isNotNull(), 1).otherwise(0)).alias("invoice_date_extracted"),
        sum(when(col("total_amount_parsed").isNotNull(), 1).otherwise(0)).alias("total_amount_extracted"),
        sum(when(col("vendor_name").isNotNull(), 1).otherwise(0)).alias("vendor_name_extracted")
    )
    .withColumn("total_records", sum("record_count").over(Window.partitionBy("_environment")))
    .withColumn("percentage", round(col("record_count") / col("total_records") * 100, 2))
    .withColumn("metric_timestamp", current_timestamp())
    .withColumn("metric_date", current_date())
    .orderBy("quality_tier")
)

# Write to Gold table
(df_quality_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_QUALITY_TABLE)
)

print(f"✓ Created {GOLD_QUALITY_TABLE}")
print(f"\nData Quality Distribution:")
display(df_quality_metrics)

# COMMAND ----------

# DBTITLE 1,Optimize Gold Tables
# MAGIC %sql
# MAGIC -- Optimize all gold tables for fast queries
# MAGIC OPTIMIZE invoice_analytics_dev.gold.invoice_summary ZORDER BY (invoice_month, vendor);
# MAGIC OPTIMIZE invoice_analytics_dev.gold.vendor_analytics ZORDER BY (total_revenue, vendor_name);
# MAGIC OPTIMIZE invoice_analytics_dev.gold.data_quality_metrics ZORDER BY (quality_tier);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics Summary Report

# COMMAND ----------

# DBTITLE 1,Analytics Summary & Sample Queries
print("="*80)
print("GOLD LAYER INVOICE ANALYTICS SUMMARY")
print("="*80)

# Load all gold tables
summary_df = spark.table(GOLD_SUMMARY_TABLE)
quality_df = spark.table(GOLD_QUALITY_TABLE)
vendor_df = spark.table(GOLD_VENDOR_TABLE)

# Bronze layer stats (CSV ingestion)
bronze_count = spark.table(f"{catalog_name}.bronze.invoices_raw_csv").count()

# Silver layer stats
silver_count = spark.table(SILVER_TABLE).count()
high_quality = spark.table(SILVER_TABLE).filter(col("_data_quality_score") >= 0.8).count()

print(f"\n📊 Pipeline Statistics (CSV Mode):")
print(f"  Bronze (CSV Records):        {bronze_count:,}")
print(f"  Silver (Transformed):        {silver_count:,}")
print(f"  High Quality Extraction:     {high_quality:,} ({high_quality/silver_count*100:.1f}%)")
print(f"  Processing Success Rate:     {silver_count/bronze_count*100:.1f}%")

print(f"\n📝 Invoice Summary:")
display(summary_df.limit(10))

print(f"\n📊 Data Quality Metrics:")
display(quality_df)

print(f"\n🏭 Vendor Analytics:")
display(vendor_df.limit(10))

print(f"\n" + "="*80)
print("SAMPLE ANALYTICS QUERIES (Deliverable)")
print("="*80)

print(f"\n-- Query 1: Monthly Invoice Trends")
print(f"SELECT invoice_month, SUM(total_invoices) as invoices, ROUND(SUM(total_amount_sum), 2) as revenue")
print(f"FROM {GOLD_SUMMARY_TABLE}")
print(f"GROUP BY invoice_month ORDER BY invoice_month DESC;")

print(f"\n-- Query 2: Top 10 Vendors by Revenue")
print(f"SELECT vendor_name, total_invoices, ROUND(total_revenue, 2) as revenue, ROUND(avg_invoice_value, 2) as avg_value")
print(f"FROM {GOLD_VENDOR_TABLE}")
print(f"ORDER BY total_revenue DESC LIMIT 10;")

print(f"\n-- Query 3: Data Quality Score Distribution")
print(f"SELECT quality_tier, record_count, percentage, avg_fields_extracted")
print(f"FROM {GOLD_QUALITY_TABLE}")
print(f"ORDER BY quality_tier;")

print(f"\n-- Query 4: Invoice Amount Analysis")
print(f"SELECT vendor, AVG(avg_invoice_amount) as avg_amount, SUM(total_invoices) as invoice_count")
print(f"FROM {GOLD_SUMMARY_TABLE}")
print(f"GROUP BY vendor HAVING invoice_count > 5 ORDER BY avg_amount DESC;")

print(f"\n✅ Gold layer processing complete!")
print(f"\n📊 Deliverables Ready:")
print(f"  ✓ Cleaned tables: Bronze (CSV) → Silver → Gold")
print(f"  ✓ Data quality report: {GOLD_QUALITY_TABLE}")
print(f"  ✓ Sample analytics queries: See above")
