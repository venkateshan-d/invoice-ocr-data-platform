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
# Read image files using READ_FILES
df_images = spark.read.format("binaryFile").load(VOLUME_PATH)

image_count = df_images.count()
print(f"✓ Loaded {image_count:,} invoice images")
print(f"✓ Processing with ai_parse_document()")

# COMMAND ----------

# DBTITLE 1,Apply OCR Transformation
# Parse documents using Databricks AI function
df_bronze = (df_images
    .withColumn("file_name", element_at(split(col("path"), "/"), -1))
    .withColumn("parsed_content", expr("ai_parse_document(content, MAP('version', '2.0'))"))
    .withColumn("has_error", col("parsed_content.error_status").isNotNull())
    .withColumn("page_count", size(col("parsed_content.document.pages")))
    .withColumn("element_count", size(col("parsed_content.document.elements")))
    .withColumn("_load_timestamp", current_timestamp())
    .withColumn("_environment", lit(environment))
    .select(
        col("path").alias("image_path"),
        col("file_name"),
        col("parsed_content"),
        col("has_error"),
        col("page_count"),
        col("element_count"),
        col("length").alias("file_size_bytes"),
        col("modificationTime").alias("file_modified_time"),
        col("_load_timestamp"),
        col("_environment")
    )
)

print("✓ Document parsing applied")

# COMMAND ----------

# DBTITLE 1,Write to Bronze Table
# Write to Bronze Delta table
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"\n✓ Bronze ingestion complete!")
print(f"✓ Data written to: {BRONZE_TABLE}")

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
