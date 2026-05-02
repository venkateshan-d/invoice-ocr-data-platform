# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# DBTITLE 1,Silver Layer - Invoice Data Extraction
# MAGIC %md
# MAGIC # Silver Layer - Invoice Data Extraction from OCR Text
# MAGIC
# MAGIC **Purpose**: Parse raw OCR text and extract structured invoice fields
# MAGIC
# MAGIC **Transformations**:
# MAGIC - Extract invoice fields using regex patterns (invoice_id, date, amount, vendor, etc.)
# MAGIC - Data type validation and standardization
# MAGIC - Data quality scoring
# MAGIC - Deduplication based on content hash
# MAGIC - Business rule validations
# MAGIC
# MAGIC **Input**: `invoice_analytics_dev.bronze.invoices_raw_ocr`
# MAGIC
# MAGIC **Output**: `invoice_analytics_dev.silver.invoices_clean`

# COMMAND ----------

# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Define Tables
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# Define tables
BRONZE_TABLE = f"{catalog_name}.bronze.invoices_raw_ocr"
SILVER_TABLE = f"{catalog_name}.silver.invoices_clean"

print(f"Source: {BRONZE_TABLE}")
print(f"Target: {SILVER_TABLE}")

# COMMAND ----------

# Read bronze data
df_bronze = spark.table(BRONZE_TABLE)

print(f"Bronze records: {df_bronze.count():,}")
print(f"Bronze schema: {len(df_bronze.columns)} columns")

# COMMAND ----------

# DBTITLE 1,Extract Invoice Fields from OCR Text
# MAGIC %md
# MAGIC ## Extract Invoice Fields from OCR Text
# MAGIC
# MAGIC Use regex patterns to extract:
# MAGIC - Invoice Number/ID
# MAGIC - Invoice Date
# MAGIC - Total Amount
# MAGIC - Vendor/Company Name
# MAGIC - Customer Details

# COMMAND ----------

# DBTITLE 1,Parse OCR Text and Extract Fields
import re

# Read bronze OCR data (only successful OCR)
df_bronze = spark.table(BRONZE_TABLE).filter(col("has_error") == False)

print(f"Bronze OCR records: {df_bronze.count():,}")

# Define regex patterns for invoice field extraction
patterns = {
    "invoice_number": r"(?:Invoice|Invoice\s*No|Invoice\s*Number|INV)[:\s#]*([A-Z0-9-]+)",
    "invoice_date": r"(?:Date|Invoice\s*Date|Dated)[:\s]*([0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4})",
    "total_amount": r"(?:Total|Amount|Grand\s*Total|Total\s*Amount)[:\s$]*([0-9,]+\.?[0-9]{0,2})",
    "vendor_name": r"^([A-Z][A-Za-z\s&]+(?:Inc|LLC|Ltd|Corporation|Corp)?)",
    "customer_name": r"(?:Bill\s*To|Customer)[:\s]*([A-Z][A-Za-z\s]+)"
}

# UDF to extract field using regex
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def extract_field(text, pattern):
    if text is None:
        return None
    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    return match.group(1).strip() if match else None

# Apply field extraction
df_extracted = df_bronze
for field_name, pattern in patterns.items():
    df_extracted = df_extracted.withColumn(field_name, extract_field(col("ocr_text"), lit(pattern)))

print("✓ Field extraction applied")

# COMMAND ----------

# DBTITLE 1,Apply Data Quality Transformations
# Clean and standardize extracted fields
df_silver = (df_extracted
    # Parse invoice date
    .withColumn("invoice_date_parsed", 
        when(col("invoice_date").isNotNull(), 
             to_date(regexp_replace(col("invoice_date"), r"[-/]", "-"), "MM-dd-yyyy"))
        .otherwise(to_date(regexp_replace(col("invoice_date"), r"[-/]", "-"), "dd-MM-yyyy")))
    
    # Parse total amount (remove commas and convert to decimal)
    .withColumn("total_amount_parsed", 
        when(col("total_amount").isNotNull(),
             regexp_replace(col("total_amount"), ",", "").cast("decimal(10,2)"))
        .otherwise(lit(None)))
    
    # Clean vendor and customer names
    .withColumn("vendor_name_clean", upper(trim(col("vendor_name"))))
    .withColumn("customer_name_clean", upper(trim(col("customer_name"))))
    
    # Calculate field completeness
    .withColumn("fields_extracted", 
        (when(col("invoice_number").isNotNull(), 1).otherwise(0) +
         when(col("invoice_date_parsed").isNotNull(), 1).otherwise(0) +
         when(col("total_amount_parsed").isNotNull(), 1).otherwise(0) +
         when(col("vendor_name").isNotNull(), 1).otherwise(0) +
         when(col("customer_name").isNotNull(), 1).otherwise(0)))
    
    # Data quality score (0.0 to 1.0)
    .withColumn("_data_quality_score", col("fields_extracted") / 5.0)
    
    # Add row hash for deduplication
    .withColumn("row_hash", 
        sha2(concat_ws("||", 
            coalesce(col("invoice_number"), lit("")),
            coalesce(col("invoice_date"), lit("")),
            coalesce(col("total_amount"), lit(""))), 256))
    
    # Add silver metadata
    .withColumn("_silver_processed_timestamp", current_timestamp())
    .withColumn("_environment", lit(environment))
)

print("✓ Data quality transformations applied")

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

# DBTITLE 1,Invoice Data Quality Report
# Data quality report for invoices
df_silver_final = spark.table(SILVER_TABLE)

print("="*80)
print("SILVER LAYER INVOICE DATA QUALITY REPORT")
print("="*80)

# Overall metrics
total_invoices = df_silver_final.count()
print(f"\nTotal invoices: {total_invoices:,}")

# Field extraction rates
field_stats = df_silver_final.agg(
    (count(when(col("invoice_number").isNotNull(), 1)) / total_invoices * 100).alias("invoice_number_pct"),
    (count(when(col("invoice_date_parsed").isNotNull(), 1)) / total_invoices * 100).alias("invoice_date_pct"),
    (count(when(col("total_amount_parsed").isNotNull(), 1)) / total_invoices * 100).alias("total_amount_pct"),
    (count(when(col("vendor_name").isNotNull(), 1)) / total_invoices * 100).alias("vendor_name_pct"),
    (count(when(col("customer_name").isNotNull(), 1)) / total_invoices * 100).alias("customer_name_pct")
).collect()[0]

print(f"\nField Extraction Rates:")
print(f"  Invoice Number: {field_stats['invoice_number_pct']:.1f}%")
print(f"  Invoice Date:   {field_stats['invoice_date_pct']:.1f}%")
print(f"  Total Amount:   {field_stats['total_amount_pct']:.1f}%")
print(f"  Vendor Name:    {field_stats['vendor_name_pct']:.1f}%")
print(f"  Customer Name:  {field_stats['customer_name_pct']:.1f}%")

# Quality score distribution
print(f"\nData Quality Score Distribution:")
quality_dist = df_silver_final.groupBy("_data_quality_score").count().orderBy("_data_quality_score")
display(quality_dist)

# High quality invoices (score >= 0.8)
high_quality_count = df_silver_final.filter(col("_data_quality_score") >= 0.8).count()
print(f"\nHigh Quality Invoices (score ≥ 0.8): {high_quality_count:,} ({high_quality_count/total_invoices*100:.1f}%)")

# Sample clean records
print(f"\nSample of Extracted Invoice Data:")
display(df_silver_final.select(
    "file_name", "invoice_number", "invoice_date_parsed", 
    "total_amount_parsed", "vendor_name_clean", "_data_quality_score"
).filter(col("_data_quality_score") >= 0.6).limit(10))
