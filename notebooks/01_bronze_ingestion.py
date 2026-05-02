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

# DBTITLE 1,Bronze Layer - Invoice Image OCR Ingestion
# MAGIC %md
# MAGIC # Bronze Layer - Invoice Image OCR Ingestion
# MAGIC
# MAGIC **Purpose**: Ingest invoice images from UC Volume, perform OCR, extract raw text
# MAGIC
# MAGIC **Features**:
# MAGIC - Auto Loader for image files (JPG, PNG, PDF)
# MAGIC - OCR text extraction using pytesseract
# MAGIC - Store raw OCR text + image metadata
# MAGIC - Schema: image_path, file_name, ocr_text, file_size, load_timestamp
# MAGIC
# MAGIC **Input**: `/Volumes/invoice_analytics_dev/bronze/raw_data/*.jpg`
# MAGIC
# MAGIC **Output**: `invoice_analytics_dev.bronze.invoices_raw_ocr` table

# COMMAND ----------

# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")

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
# MAGIC CREATE OR REPLACE TABLE invoice_analytics_dev.bronze.invoices_raw_ocr
# MAGIC AS
# MAGIC SELECT
# MAGIC   path AS image_path,
# MAGIC   regexp_extract(path, '[^/]+$', 0) AS file_name,
# MAGIC   ai_parse_document(content, MAP('version', '2.0')) AS parsed_content,
# MAGIC   try_cast(ai_parse_document(content, MAP('version', '2.0')):error_status AS STRING) IS NOT NULL AS has_error,
# MAGIC   size(try_cast(ai_parse_document(content, MAP('version', '2.0')):document:pages AS ARRAY<VARIANT>)) AS page_count,
# MAGIC   size(try_cast(ai_parse_document(content, MAP('version', '2.0')):document:elements AS ARRAY<VARIANT>)) AS element_count,
# MAGIC   length AS file_size_bytes,
# MAGIC   modificationTime AS file_modified_time,
# MAGIC   current_timestamp() AS _load_timestamp,
# MAGIC   'dev' AS _environment
# MAGIC FROM READ_FILES(
# MAGIC   '/Volumes/invoice_analytics_dev/bronze/raw_data/',
# MAGIC   format => 'binaryFile'
# MAGIC );

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
