# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,⚠️ IMPORTANT: Run Order
# MAGIC %md
# MAGIC # Bronze Layer - OCR Processing
# MAGIC
# MAGIC Parses invoice images using Databricks AI functions.

# COMMAND ----------

# DBTITLE 1,Parameters
# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("test_mode", "false", "Test Mode (true = 100 images only)")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")
test_mode = dbutils.widgets.get("test_mode").lower() == "true"

print("="*80)
print("BRONZE LAYER - OCR PROCESSING")
print("="*80)
print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Test Mode: {test_mode}")
if test_mode:
    print(f"\n🧪 TEST MODE: Processing 100 images for validation")
    print(f"   Estimated time: ~2-3 minutes")
else:
    print(f"\n⚡ PRODUCTION MODE: Processing ALL images (8,137)")
    print(f"   Serverless will auto-scale for parallel AI calls")
    print(f"   Estimated time: ~30-45 minutes")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Define Paths
from pyspark.sql.functions import *

VOLUME_PATH = f"/Volumes/{catalog_name}/bronze/raw_data/"
CHECKPOINT_PATH = f"/Volumes/{catalog_name}/bronze/checkpoints/bronze_ocr/"
BRONZE_TABLE = f"{catalog_name}.bronze.invoices_raw_ocr"

print(f"Source Images: {VOLUME_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print(f"Target Table: {BRONZE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read Images with Auto Loader
# Build SQL query with optional LIMIT for test mode
limit_clause = "LIMIT 100" if test_mode else ""

sql_query = f"""
CREATE OR REPLACE TABLE {catalog_name}.bronze.invoices_raw_ocr
AS
WITH image_batch AS (
  SELECT
    path,
    regexp_extract(path, '[^/]+$', 0) AS file_name,
    content,
    length AS file_size_bytes,
    modificationTime AS file_modified_time
  FROM READ_FILES(
    '/Volumes/{catalog_name}/bronze/raw_data/',
    format => 'binaryFile'
  )
  {limit_clause}
),
parsed_docs AS (
  SELECT
    path,
    file_name,
    file_size_bytes,
    file_modified_time,
    ai_parse_document(content, MAP('version', '2.0')) AS parsed_content
  FROM image_batch
)
SELECT
  path AS image_path,
  file_name,
  parsed_content,
  try_cast(parsed_content:error_status AS STRING) IS NOT NULL AS has_error,
  size(try_cast(parsed_content:document:pages AS ARRAY<VARIANT>)) AS page_count,
  size(try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)) AS element_count,
  file_size_bytes,
  file_modified_time,
  current_timestamp() AS _load_timestamp,
  '{environment}' AS _environment
FROM parsed_docs
"""

print(f"\n⚡ Executing OCR processing...")
if test_mode:
    print(f"   Mode: TEST (100 images)")
else:
    print(f"   Mode: PRODUCTION (all images)")

result_df = spark.sql(sql_query)
print(f"\n✅ OCR processing complete!")

# COMMAND ----------

# DBTITLE 1,Optimize Bronze Table
# MAGIC %sql
# MAGIC -- Optimize and Z-order for faster queries
# MAGIC OPTIMIZE invoice_analytics_dev.bronze.invoices_raw_ocr
# MAGIC ZORDER BY (has_error, file_name);

# COMMAND ----------

# DBTITLE 1,Write to Bronze Table
# Comprehensive verification
df_bronze_table = spark.table(BRONZE_TABLE)

print(f"\n✅ Bronze ingestion complete!")
print(f"✅ Data written to: {BRONZE_TABLE}")

total_records = df_bronze_table.count()
print(f"\n📊 Total images processed: {total_records:,}")

if total_records > 0:
    error_count = df_bronze_table.filter(col("has_error") == True).count()
    success_count = total_records - error_count
    success_rate = (success_count / total_records * 100)
    print(f"   ✓ Successfully parsed: {success_count:,} ({success_rate:.1f}%)")
    print(f"   ⚠️  Parse errors: {error_count:,} ({100-success_rate:.1f}%)")
    
    if success_count > 0:
        stats = df_bronze_table.filter(col("has_error") == False).agg(
            avg("page_count").alias("avg_pages"),
            avg("element_count").alias("avg_elements"),
            avg("file_size_bytes").alias("avg_size")
        ).collect()[0]
        print(f"\n📊 Parsing Quality:")
        print(f"   Avg pages per document: {stats['avg_pages']:.1f}")
        print(f"   Avg elements extracted: {stats['avg_elements']:.1f}")
        print(f"   Avg file size: {stats['avg_size']/1024:.1f} KB")
    
    if test_mode:
        print(f"\n🧪 Test mode complete - {total_records} records processed")
        print(f"   Set test_mode=false to process all 8,137 images")
    else:
        print(f"\n✅ Production processing complete - {total_records:,} records")
else:
    print("   ⚠️  No records found - check source path")

# COMMAND ----------

# DBTITLE 1,Verify OCR Results
# Detailed statistics and sample data
df_bronze_table = spark.table(BRONZE_TABLE)

print("="*80)
print("BRONZE LAYER - DOCUMENT PARSING REPORT")
print("="*80)

row_count = df_bronze_table.count()
error_count = df_bronze_table.filter(col("has_error") == True).count()
success_count = row_count - error_count
success_rate = (success_count / row_count * 100) if row_count > 0 else 0

print(f"\n📄 Processing Summary:")
print(f"   Total images: {row_count:,}")
print(f"   Successful: {success_count:,} ({success_rate:.1f}%)")
print(f"   Errors: {error_count:,} ({100-success_rate:.1f}%)")

if success_count > 0:
    stats = df_bronze_table.filter(col("has_error") == False).agg(
        avg("page_count").alias("avg_pages"),
        max("page_count").alias("max_pages"),
        avg("element_count").alias("avg_elements"),
        max("element_count").alias("max_elements"),
        avg("file_size_bytes").alias("avg_size")
    ).collect()[0]
    
    print(f"\n📊 Quality Metrics:")
    print(f"   Avg pages: {stats['avg_pages']:.1f} (max: {stats['max_pages']})")
    print(f"   Avg elements: {stats['avg_elements']:.1f} (max: {stats['max_elements']})")
    print(f"   Avg file size: {stats['avg_size']/1024:.1f} KB")

if error_count > 0:
    print(f"\n⚠️ Error Analysis:")
    print(f"   {error_count:,} images failed to parse")
    print(f"   Check error_status field for details")

print(f"\n✅ Ready for silver transformation")
print(f"   Next: Run 02_silver_transformation notebook")
print("="*80)

# Show sample successful parses
if success_count > 0:
    print("\n👁️ Sample Successfully Parsed Documents:")
    display(df_bronze_table.filter(col("has_error") == False)
            .select("file_name", "page_count", "element_count", "file_size_bytes")
            .orderBy(desc("element_count"))
            .limit(10))
