# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,⚠️ IMPORTANT: Run Order
# MAGIC %md
# MAGIC # ⚠️ IMPORTANT: Execution Order
# MAGIC
# MAGIC **Serverless-Compatible OCR**
# MAGIC
# MAGIC This notebook uses **Databricks AI functions** (`ai_parse_document`) for document parsing, which works natively on serverless compute without requiring Tesseract installation.
# MAGIC
# MAGIC **Simply run all cells in order.**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,OCR UDF Function
# MAGIC %md
# MAGIC # Bronze Layer - Invoice Document Parsing with AI
# MAGIC
# MAGIC **Purpose**: Parse invoice images using Databricks AI functions
# MAGIC
# MAGIC **Method**: Uses `ai_parse_document()` to extract structured content from images
# MAGIC - **No Tesseract required** - works on serverless compute
# MAGIC - Extracts text, tables, and document structure
# MAGIC - Stores parsed VARIANT for downstream processing
# MAGIC
# MAGIC **Input**: `/Volumes/invoice_analytics_dev/bronze/raw_data/*.jpg`
# MAGIC
# MAGIC **Output**: `invoice_analytics_dev.bronze.invoices_raw_ocr`

# COMMAND ----------

# DBTITLE 1,Parameters
# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("batch_size", "100", "Batch Size (0 = all images)")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Batch Size: {batch_size if batch_size > 0 else 'ALL IMAGES'}")

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
# MAGIC %sql
# MAGIC -- OPTIMIZED: Process 100 images for testing (change batch_size parameter for more)
# MAGIC -- Full batch (8,137 images) will take ~4-6 hours on serverless
# MAGIC -- This test batch should complete in ~10-15 minutes
# MAGIC
# MAGIC CREATE OR REPLACE TABLE invoice_analytics_dev.bronze.invoices_raw_ocr
# MAGIC AS
# MAGIC WITH image_batch AS (
# MAGIC   SELECT
# MAGIC     path,
# MAGIC     regexp_extract(path, '[^/]+$', 0) AS file_name,
# MAGIC     content,
# MAGIC     length AS file_size_bytes,
# MAGIC     modificationTime AS file_modified_time
# MAGIC   FROM READ_FILES(
# MAGIC     '/Volumes/invoice_analytics_dev/bronze/raw_data/',
# MAGIC     format => 'binaryFile'
# MAGIC   )
# MAGIC   LIMIT 100  -- TESTING MODE: Process 100 images first
# MAGIC ),
# MAGIC parsed_docs AS (
# MAGIC   SELECT
# MAGIC     path,
# MAGIC     file_name,
# MAGIC     file_size_bytes,
# MAGIC     file_modified_time,
# MAGIC     ai_parse_document(content, MAP('version', '2.0')) AS parsed_content
# MAGIC   FROM image_batch
# MAGIC )
# MAGIC SELECT
# MAGIC   path AS image_path,
# MAGIC   file_name,
# MAGIC   parsed_content,
# MAGIC   try_cast(parsed_content:error_status AS STRING) IS NOT NULL AS has_error,
# MAGIC   size(try_cast(parsed_content:document:pages AS ARRAY<VARIANT>)) AS page_count,
# MAGIC   size(try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)) AS element_count,
# MAGIC   file_size_bytes,
# MAGIC   file_modified_time,
# MAGIC   current_timestamp() AS _load_timestamp,
# MAGIC   'dev' AS _environment
# MAGIC FROM parsed_docs;

# COMMAND ----------

# DBTITLE 1,📊 Processing Full Dataset
# MAGIC %md
# MAGIC ## 📊 Processing Full Dataset (8,137 images)
# MAGIC
# MAGIC **Current Status**: Testing mode - processing **100 images** (~10-15 min)
# MAGIC
# MAGIC ### To Process ALL Images:
# MAGIC
# MAGIC **Option 1: Batch Processing (RECOMMENDED)**
# MAGIC ```sql
# MAGIC -- Process in batches of 500 images to avoid timeouts
# MAGIC -- Run this cell multiple times, changing OFFSET each time
# MAGIC
# MAGIC INSERT INTO invoice_analytics_dev.bronze.invoices_raw_ocr
# MAGIC SELECT * FROM (
# MAGIC   WITH image_batch AS (
# MAGIC     SELECT path, content, length, modificationTime,
# MAGIC            regexp_extract(path, '[^/]+$', 0) AS file_name,
# MAGIC            ROW_NUMBER() OVER (ORDER BY path) as rn
# MAGIC     FROM READ_FILES('/Volumes/invoice_analytics_dev/bronze/raw_data/', format => 'binaryFile')
# MAGIC   )
# MAGIC   SELECT
# MAGIC     path AS image_path,
# MAGIC     file_name,
# MAGIC     ai_parse_document(content, MAP('version', '2.0')) AS parsed_content,
# MAGIC     ...
# MAGIC   FROM image_batch
# MAGIC   WHERE rn BETWEEN 101 AND 600  -- Batch 2: images 101-600
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Option 2: Remove LIMIT**
# MAGIC - Change `LIMIT 100` to `LIMIT 8137` in Cell 6
# MAGIC - Expected time: **4-6 hours** on serverless
# MAGIC - Run during off-hours
# MAGIC
# MAGIC **Option 3: Use Databricks Jobs**
# MAGIC - Schedule the job to run overnight
# MAGIC - Set timeout to 8 hours
# MAGIC - Enable auto-retry on failure

# COMMAND ----------

# DBTITLE 1,Optimize Bronze Table
# MAGIC %sql
# MAGIC -- Optimize and Z-order for faster queries
# MAGIC OPTIMIZE invoice_analytics_dev.bronze.invoices_raw_ocr
# MAGIC ZORDER BY (has_error, file_name);

# COMMAND ----------

# DBTITLE 1,Write to Bronze Table
# Verify table was created
print(f"\n✓ Bronze ingestion complete!")
print(f"✓ Data written to: {BRONZE_TABLE}")
print(f"\nVerifying table...")
row_count = spark.table(BRONZE_TABLE).count()
print(f"✓ Table contains {row_count:,} records")

# COMMAND ----------

# DBTITLE 1,Verify OCR Results
# Verify document parsing
df_bronze_table = spark.table(BRONZE_TABLE)
row_count = df_bronze_table.count()
error_count = df_bronze_table.filter(col("has_error") == True).count()
success_count = row_count - error_count
success_rate = (success_count / row_count * 100) if row_count > 0 else 0
avg_page_count = df_bronze_table.agg(avg("page_count")).collect()[0][0]
avg_element_count = df_bronze_table.agg(avg("element_count")).collect()[0][0]

print("="*80)
print("BRONZE LAYER DOCUMENT PARSING STATISTICS")
print("="*80)
print(f"Total documents processed: {row_count:,}")
print(f"Parsing successful: {success_count:,}")
print(f"Parsing errors: {error_count:,}")
print(f"Success rate: {success_rate:.2f}%")
print(f"Avg pages per document: {avg_page_count:.1f}")
print(f"Avg elements extracted: {avg_element_count:.1f}")
print(f"Table: {BRONZE_TABLE}")
print("="*80)

# Show sample parsed documents
print("\nSample Parsed Documents:")
display(df_bronze_table.select("file_name", "page_count", "element_count", "has_error").limit(10))
