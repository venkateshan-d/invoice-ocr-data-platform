# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,⚠️ IMPORTANT: Run Order
# MAGIC %md
# MAGIC # ⚠️ IMPORTANT: Execution Order
# MAGIC
# MAGIC **Before running this notebook:**
# MAGIC
# MAGIC 1. **First**, run **Cell 7 (Install OCR Dependencies)** to install pytesseract and PIL
# MAGIC 2. **Then**, run cells in order from Cell 1 onwards
# MAGIC
# MAGIC **Or**: Run all cells - the notebook will restart Python after installing dependencies
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,OCR UDF Function
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pytesseract
from PIL import Image
import io

@udf(returnType=StringType())
def extract_text_from_image(image_bytes):
    try:
        if image_bytes is None:
            return None
        image = Image.open(io.BytesIO(image_bytes))
        text = pytesseract.image_to_string(image)
        return text.strip()
    except Exception as e:
        return f"OCR_ERROR: {str(e)}"

print("✓ OCR function registered")

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
# Read image files with Auto Loader (binaryFile format)
df_images = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.{jpg,jpeg,png,JPG,JPEG,PNG}")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema")
    .load(VOLUME_PATH)
    .withColumn("file_name", element_at(split(col("path"), "/"), -1))
    .withColumn("_load_timestamp", current_timestamp())
    .withColumn("_environment", lit(environment))
)

print("✓ Auto Loader configured for images")

# COMMAND ----------

# DBTITLE 1,Apply OCR Transformation
# Apply OCR to extract text from images
df_bronze = (df_images
    .withColumn("ocr_text", extract_text_from_image(col("content")))
    .withColumn("text_length", length(col("ocr_text")))
    .withColumn("has_error", col("ocr_text").startswith("OCR_ERROR"))
    .select(
        col("path").alias("image_path"),
        col("file_name"),
        col("ocr_text"),
        col("text_length"),
        col("has_error"),
        col("length").alias("file_size_bytes"),
        col("modificationTime").alias("file_modified_time"),
        col("_load_timestamp"),
        col("_environment")
    )
)

print("✓ OCR transformation applied")

# COMMAND ----------

# DBTITLE 1,Install OCR Dependencies
# Install pytesseract and PIL for OCR
%pip install pytesseract pillow --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Write to Bronze Delta table with checkpointing
query = (df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available files then stop
    .table(BRONZE_TABLE)
)

query.awaitTermination()

print(f"\n✓ Bronze ingestion complete!")
print(f"✓ Data written to: {BRONZE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verify OCR Results
# Verify OCR ingestion
df_bronze_table = spark.table(BRONZE_TABLE)
row_count = df_bronze_table.count()
error_count = df_bronze_table.filter(col("has_error") == True).count()
success_count = row_count - error_count
success_rate = (success_count / row_count * 100) if row_count > 0 else 0
avg_text_len = df_bronze_table.agg(avg("text_length")).collect()[0][0]

print("="*80)
print("BRONZE LAYER OCR STATISTICS")
print("="*80)
print(f"Total images processed: {row_count:,}")
print(f"OCR successful: {success_count:,}")
print(f"OCR errors: {error_count:,}")
print(f"Success rate: {success_rate:.2f}%")
print(f"Avg text length: {avg_text_len:.0f} characters")
print(f"Table: {BRONZE_TABLE}")
print("="*80)

display(df_bronze_table.select("file_name", "text_length", "has_error", "ocr_text").limit(10))
