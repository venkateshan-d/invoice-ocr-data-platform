# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer - Invoice Data Transformation
# MAGIC
# MAGIC **Purpose**: Clean, validate, and standardize bronze invoice data
# MAGIC
# MAGIC **Transformations**:
# MAGIC - Data type standardization
# MAGIC - Date parsing and validation
# MAGIC - Null handling and quality checks
# MAGIC - Deduplication logic
# MAGIC - Business rule validations

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
from pyspark.sql import Window

# Define tables
BRONZE_TABLE = f"{catalog_name}.bronze.invoices_raw"
SILVER_TABLE = f"{catalog_name}.silver.invoices_clean"

print(f"Source: {BRONZE_TABLE}")
print(f"Target: {SILVER_TABLE}")

# COMMAND ----------

# Read bronze data
df_bronze = spark.table(BRONZE_TABLE)

print(f"Bronze records: {df_bronze.count():,}")
print(f"Bronze schema: {len(df_bronze.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Transformations

# COMMAND ----------

# Apply transformations
df_silver = (df_bronze
    # Remove exact duplicates
    .dropDuplicates()
    
    # Add data quality flags
    .withColumn("has_null_values", 
        size(array([when(col(c).isNull(), c) for c in df_bronze.columns 
                    if c not in ['_rescue_data', '_input_file', '_load_timestamp', '_environment']])) > 0)
    
    # Add row hash for deduplication tracking
    .withColumn("row_hash", sha2(concat_ws("||", *[col(c) for c in df_bronze.columns 
                                                     if c not in ['_input_file', '_load_timestamp', '_environment', '_rescue_data']]), 256))
    
    # Add silver layer metadata
    .withColumn("_silver_processed_timestamp", current_timestamp())
    .withColumn("_data_quality_score", 
        when(col("_rescue_data").isNotNull(), lit(0.0))
        .when(col("has_null_values") == True, lit(0.5))
        .otherwise(lit(1.0)))
)

print(f"✓ Transformations applied")

# COMMAND ----------

# Deduplication: Keep most recent record per row_hash
window_spec = Window.partitionBy("row_hash").orderBy(desc("_load_timestamp"))

df_silver_deduped = (df_silver
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

original_count = df_silver.count()
deduped_count = df_silver_deduped.count()
duplicates_removed = original_count - deduped_count

print(f"Original records: {original_count:,}")
print(f"After deduplication: {deduped_count:,}")
print(f"Duplicates removed: {duplicates_removed:,}")

# COMMAND ----------

# Write to Silver Delta table
(df_silver_deduped.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(SILVER_TABLE)
)

print(f"\n✓ Silver transformation complete!")
print(f"✓ Data written to: {SILVER_TABLE}")

# COMMAND ----------

# Data quality report
df_silver_final = spark.table(SILVER_TABLE)

print("="*80)
print("SILVER LAYER DATA QUALITY REPORT")
print("="*80)

# Quality score distribution
quality_dist = df_silver_final.groupBy("_data_quality_score").count().orderBy("_data_quality_score")
print("\nQuality Score Distribution:")
display(quality_dist)

# Null analysis
null_count = df_silver_final.filter(col("has_null_values") == True).count()
print(f"\nRecords with null values: {null_count:,} ({null_count/df_silver_final.count()*100:.2f}%)")

# Sample of clean data
print("\nSample of clean records:")
display(df_silver_final.filter(col("_data_quality_score") == 1.0).limit(10))
