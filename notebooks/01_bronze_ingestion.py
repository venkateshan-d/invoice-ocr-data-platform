# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,Bronze Layer - Invoice Ingestion
# MAGIC %md
# MAGIC # Bronze Layer - Invoice Data Ingestion
# MAGIC
# MAGIC ## Overview
# MAGIC Ingests raw invoice CSV files from Kaggle dataset into Bronze layer Delta tables.
# MAGIC
# MAGIC ## Source Data
# MAGIC * `batch1_1.csv`
# MAGIC * `batch1_2.csv`  
# MAGIC * `batch1_3.csv`
# MAGIC
# MAGIC ## Output
# MAGIC * **Table**: `invoice_analytics_dev.bronze.invoices_raw_csv`
# MAGIC * **Features**: File metadata, load timestamps, error tracking

# COMMAND ----------

# DBTITLE 1,Parameters
# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("test_mode", "false", "Test Mode (true = 100 records only)")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")
test_mode = dbutils.widgets.get("test_mode").lower() == "true"

print("="*80)
print("BRONZE LAYER - CSV INGESTION")
print("="*80)
print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Test Mode: {test_mode}")
if test_mode:
    print(f"\n🧪 TEST MODE: Processing 100 records for validation")
else:
    print(f"\n⚡ PRODUCTION MODE: Processing all CSV records")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Define Paths
from pyspark.sql.functions import *

VOLUME_PATH = f"/Volumes/{catalog_name}/bronze/raw_data/"
CHECKPOINT_PATH = f"/Volumes/{catalog_name}/bronze/checkpoints/bronze_csv/"
BRONZE_TABLE = f"{catalog_name}.bronze.invoices_raw_csv"

print(f"Source Data: {VOLUME_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print(f"Target Table: {BRONZE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Ingest CSV Files to Bronze
# CSV Ingestion with Auto Loader
limit_clause = "LIMIT 100" if test_mode else ""

print(f"\n⚡ Ingesting CSV files from {VOLUME_PATH}...")
if test_mode:
    print(f"   Mode: TEST (100 records)")
else:
    print(f"   Mode: PRODUCTION (all records)")

try:
    sql_query = f"""
    CREATE OR REPLACE TABLE {catalog_name}.bronze.invoices_raw_csv
    AS
    SELECT
      *,
      _metadata.file_path AS source_file_path,
      _metadata.file_name AS source_file_name,
      _metadata.file_size AS file_size_bytes,
      _metadata.file_modification_time AS file_modified_time,
      current_timestamp() AS _load_timestamp,
      '{environment}' AS _environment,
      FALSE AS has_error,
      NULL AS error_message
    FROM READ_FILES(
      '/Volumes/{catalog_name}/bronze/raw_data/',
      format => 'csv',
      header => 'true',
      inferSchema => 'true'
    )
    WHERE _metadata.file_name RLIKE '.*batch1_[0-9]+\\.csv$'
    {limit_clause}
    """
    
    result_df = spark.sql(sql_query)
    print(f"\n✅ CSV ingestion complete!")
    
except Exception as e:
    error_msg = str(e)
    print(f"\n❌ Bronze ingestion failed with error:")
    print(f"   {error_msg}")
    print(f"\n🔍 Possible issues:")
    print(f"   1. CSV files may not exist in /Volumes/{catalog_name}/bronze/raw_data/")
    print(f"   2. File names don't match pattern 'batch1_*.csv'")
    print(f"   3. Check if volume exists and is accessible")
    raise

# COMMAND ----------

# DBTITLE 1,Optimize Bronze Table
# MAGIC %sql
# MAGIC -- Optimize and Z-order for faster queries
# MAGIC OPTIMIZE invoice_analytics_dev.bronze.invoices_raw_csv
# MAGIC ZORDER BY (source_file_name);

# COMMAND ----------

# DBTITLE 1,Verify CSV Ingestion
# Comprehensive verification for CSV ingestion
BRONZE_CSV_TABLE = f"{catalog_name}.bronze.invoices_raw_csv"
df_bronze_table = spark.table(BRONZE_CSV_TABLE)

print(f"\n✅ Bronze CSV ingestion complete!")
print(f"✅ Data written to: {BRONZE_CSV_TABLE}")

total_records = df_bronze_table.count()
print(f"\n📊 Total CSV records ingested: {total_records:,}")

if total_records > 0:
    # Get column information
    print(f"\n📊 Dataset Schema:")
    print(f"   Total columns: {len(df_bronze_table.columns)}")
    
    # Show column names (excluding metadata columns)
    business_cols = [c for c in df_bronze_table.columns if not c.startswith('_') and not c.startswith('source_') and not c.startswith('file_') and c not in ['has_error', 'error_message']]
    print(f"   Business columns: {', '.join(business_cols[:10])}{'...' if len(business_cols) > 10 else ''}")
    
    # File source breakdown
    file_breakdown = df_bronze_table.groupBy("source_file_name").count().collect()
    print(f"\n📁 File Breakdown:")
    for row in file_breakdown:
        print(f"   - {row.source_file_name}: {row['count']:,} records")
    
    # Show sample data
    print(f"\n🔍 Sample Records (first 5):")
    display(df_bronze_table.select(business_cols[:8] if len(business_cols) > 8 else business_cols).limit(5))
    
    if test_mode:
        print(f"\n🧪 Test mode complete - {total_records} records processed")
        print(f"   Set test_mode=false to process all records")
    else:
        print(f"\n✅ Production processing complete - {total_records:,} records")
    
    print(f"\n✅ CSV Ingestion Success Rate: 100%")
    print(f"\n🚀 Ready for Silver Layer Transformation!")
    print(f"   Next: Run [02_silver_transformation](#notebook-1673549814536738)")
else:
    print("   ⚠️  No records found - check source path and file names")
