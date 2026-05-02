# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# DBTITLE 1,Silver Layer - Invoice Data Extraction
# MAGIC %md
# MAGIC # Silver Layer - Field Extraction
# MAGIC
# MAGIC Extracts structured invoice fields using AI.

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

# DBTITLE 1,Read Bronze Data
# Read bronze data (successfully parsed documents only)
df_bronze = spark.table(BRONZE_TABLE).filter(col("has_error") == False)

print(f"Bronze records: {df_bronze.count():,}")
print(f"Bronze schema: {len(df_bronze.columns)} columns")

# COMMAND ----------

# DBTITLE 1,Extract Invoice Fields from OCR Text
# MAGIC %md
# MAGIC ## Extract Invoice Fields with AI

# COMMAND ----------

# DBTITLE 1,Parse OCR Text and Extract Fields
# MAGIC %sql
# MAGIC -- Optimized: Single SQL query with ai_extract (VARIANT field access fixed)
# MAGIC CREATE OR REPLACE TABLE invoice_analytics_dev.silver.invoices_clean
# MAGIC AS
# MAGIC WITH extracted AS (
# MAGIC   SELECT
# MAGIC     image_path,
# MAGIC     file_name,
# MAGIC     parsed_content,
# MAGIC     page_count,
# MAGIC     element_count,
# MAGIC     ai_extract(
# MAGIC       parsed_content,
# MAGIC       '{
# MAGIC         "invoice_number": {"type": "string", "description": "Invoice or reference number"},
# MAGIC         "invoice_date": {"type": "string", "description": "Invoice date in any format"},
# MAGIC         "total_amount": {"type": "number", "description": "Total invoice amount or grand total"},
# MAGIC         "vendor_name": {"type": "string", "description": "Vendor, seller, or company name"},
# MAGIC         "customer_name": {"type": "string", "description": "Customer, buyer, or bill-to name"}
# MAGIC       }',
# MAGIC       MAP('version', '2.0', 'instructions', 'Extract invoice details from this document.')
# MAGIC     ) AS extracted_fields,
# MAGIC     _load_timestamp,
# MAGIC     _environment
# MAGIC   FROM invoice_analytics_dev.bronze.invoices_raw_ocr
# MAGIC   WHERE has_error = FALSE
# MAGIC ),
# MAGIC transformed AS (
# MAGIC   SELECT
# MAGIC     image_path,
# MAGIC     file_name,
# MAGIC     -- Use COLON notation for VARIANT type (not dot notation)
# MAGIC     try_cast(extracted_fields:invoice_number AS STRING) AS invoice_number,
# MAGIC     try_cast(extracted_fields:invoice_date AS STRING) AS invoice_date,
# MAGIC     try_cast(extracted_fields:total_amount AS DOUBLE) AS total_amount,
# MAGIC     try_cast(extracted_fields:vendor_name AS STRING) AS vendor_name,
# MAGIC     try_cast(extracted_fields:customer_name AS STRING) AS customer_name,
# MAGIC     -- Parse date (multiple formats)
# MAGIC     COALESCE(
# MAGIC       try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'MM/dd/yyyy'),
# MAGIC       try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'dd/MM/yyyy'),
# MAGIC       try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'yyyy-MM-dd'),
# MAGIC       try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'MM-dd-yyyy'),
# MAGIC       try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'dd-MM-yyyy')
# MAGIC     ) AS invoice_date_parsed,
# MAGIC     -- Cast amount
# MAGIC     CAST(try_cast(extracted_fields:total_amount AS DOUBLE) AS DECIMAL(10,2)) AS total_amount_parsed,
# MAGIC     -- Clean names
# MAGIC     UPPER(TRIM(try_cast(extracted_fields:vendor_name AS STRING))) AS vendor_name_clean,
# MAGIC     UPPER(TRIM(try_cast(extracted_fields:customer_name AS STRING))) AS customer_name_clean,
# MAGIC     -- Field completeness
# MAGIC     (
# MAGIC       CASE WHEN try_cast(extracted_fields:invoice_number AS STRING) IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN COALESCE(
# MAGIC         try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'MM/dd/yyyy'),
# MAGIC         try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'dd/MM/yyyy'),
# MAGIC         try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'yyyy-MM-dd')
# MAGIC       ) IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN try_cast(extracted_fields:total_amount AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN try_cast(extracted_fields:vendor_name AS STRING) IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN try_cast(extracted_fields:customer_name AS STRING) IS NOT NULL THEN 1 ELSE 0 END
# MAGIC     ) AS fields_extracted,
# MAGIC     -- Row hash for deduplication
# MAGIC     sha2(CONCAT_WS('||',
# MAGIC       COALESCE(try_cast(extracted_fields:invoice_number AS STRING), ''),
# MAGIC       COALESCE(try_cast(extracted_fields:invoice_date AS STRING), ''),
# MAGIC       COALESCE(CAST(try_cast(extracted_fields:total_amount AS DOUBLE) AS STRING), '')
# MAGIC     ), 256) AS row_hash,
# MAGIC     current_timestamp() AS _silver_processed_timestamp,
# MAGIC     'dev' AS _environment
# MAGIC   FROM extracted
# MAGIC ),
# MAGIC deduped AS (
# MAGIC   SELECT *,
# MAGIC     fields_extracted / 5.0 AS _data_quality_score,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY _silver_processed_timestamp DESC) AS row_num
# MAGIC   FROM transformed
# MAGIC )
# MAGIC SELECT
# MAGIC   image_path, file_name, invoice_number, invoice_date, total_amount, vendor_name, customer_name,
# MAGIC   invoice_date_parsed, total_amount_parsed, vendor_name_clean, customer_name_clean,
# MAGIC   fields_extracted, _data_quality_score, row_hash, _silver_processed_timestamp, _environment
# MAGIC FROM deduped
# MAGIC WHERE row_num = 1;

# COMMAND ----------

# DBTITLE 1,Optimize Silver Table
# MAGIC %sql
# MAGIC -- Optimize and Z-order for faster queries
# MAGIC OPTIMIZE invoice_analytics_dev.silver.invoices_clean
# MAGIC ZORDER BY (vendor_name_clean, invoice_date_parsed, _data_quality_score);

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
