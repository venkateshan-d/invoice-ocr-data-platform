# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# DBTITLE 1,Silver Layer - Invoice Data Extraction
# MAGIC %md
# MAGIC # Silver Layer - Invoice Data Extraction with AI
# MAGIC
# MAGIC **Purpose**: Extract structured invoice fields from parsed documents using AI
# MAGIC
# MAGIC **Method**: Chain `ai_parse_document` → `ai_extract` for intelligent field extraction
# MAGIC - Extracts invoice_number, date, total_amount, vendor, customer
# MAGIC - Validates and standardizes data types
# MAGIC - Calculates data quality scores
# MAGIC - Deduplicates records
# MAGIC
# MAGIC **Input**: `invoice_analytics_dev.bronze.invoices_raw_ocr` (parsed documents)
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

# DBTITLE 1,Read Bronze Data
# Read bronze data (successfully parsed documents only)
df_bronze = spark.table(BRONZE_TABLE).filter(col("has_error") == False)

print(f"Bronze records: {df_bronze.count():,}")
print(f"Bronze schema: {len(df_bronze.columns)} columns")

# COMMAND ----------

# DBTITLE 1,Extract Invoice Fields from OCR Text
# MAGIC %md
# MAGIC ## Extract Invoice Fields with AI
# MAGIC
# MAGIC Use `ai_extract()` to intelligently extract:
# MAGIC - Invoice Number
# MAGIC - Invoice Date  
# MAGIC - Total Amount
# MAGIC - Vendor Name
# MAGIC - Customer Name
# MAGIC
# MAGIC The AI understands document structure and can handle various invoice formats.

# COMMAND ----------

# DBTITLE 1,Parse OCR Text and Extract Fields
# Define invoice schema for extraction
invoice_schema = '''
{
  "invoice_number": {"type": "string", "description": "Invoice or reference number"},
  "invoice_date": {"type": "string", "description": "Invoice date in any format"},
  "total_amount": {"type": "number", "description": "Total invoice amount or grand total"},
  "vendor_name": {"type": "string", "description": "Vendor, seller, or company name"},
  "customer_name": {"type": "string", "description": "Customer, buyer, or bill-to name"}
}
'''

# Extract structured fields using AI
df_extracted = df_bronze.withColumn(
    "extracted_fields",
    expr(f"""
        ai_extract(
            parsed_content,
            '{invoice_schema}',
            MAP('version', '2.0', 'instructions', 'Extract invoice details from this document.')
        )
    """)
)

print("✓ AI extraction applied")
print(f"Records to process: {df_extracted.count():,}")

# COMMAND ----------

# DBTITLE 1,Apply Data Quality Transformations
# Parse extracted JSON and create typed columns
df_silver = (df_extracted
    # Extract fields from JSON
    .withColumn("invoice_number", col("extracted_fields.invoice_number"))
    .withColumn("invoice_date", col("extracted_fields.invoice_date"))
    .withColumn("total_amount", col("extracted_fields.total_amount"))
    .withColumn("vendor_name", col("extracted_fields.vendor_name"))
    .withColumn("customer_name", col("extracted_fields.customer_name"))
    
    # Parse invoice date (try multiple formats)
    .withColumn("invoice_date_parsed",
        coalesce(
            to_date(col("invoice_date"), "MM/dd/yyyy"),
            to_date(col("invoice_date"), "dd/MM/yyyy"),
            to_date(col("invoice_date"), "yyyy-MM-dd"),
            to_date(col("invoice_date"), "MM-dd-yyyy"),
            to_date(col("invoice_date"), "dd-MM-yyyy")
        ))
    
    # Clean and cast total amount
    .withColumn("total_amount_parsed",
        when(col("total_amount").isNotNull(),
             col("total_amount").cast("decimal(10,2)"))
        .otherwise(lit(None)))
    
    # Clean vendor and customer names
    .withColumn("vendor_name_clean", upper(trim(col("vendor_name"))))
    .withColumn("customer_name_clean", upper(trim(col("customer_name"))))
    
    # Calculate field completeness (0-5)
    .withColumn("fields_extracted",
        (when(col("invoice_number").isNotNull(), 1).otherwise(0) +
         when(col("invoice_date_parsed").isNotNull(), 1).otherwise(0) +
         when(col("total_amount_parsed").isNotNull(), 1).otherwise(0) +
         when(col("vendor_name").isNotNull(), 1).otherwise(0) +
         when(col("customer_name").isNotNull(), 1).otherwise(0)))
    
    # Data quality score (0.0 to 1.0)
    .withColumn("_data_quality_score", col("fields_extracted") / 5.0)
    
    # Row hash for deduplication
    .withColumn("row_hash",
        sha2(concat_ws("||",
            coalesce(col("invoice_number"), lit("")),
            coalesce(col("invoice_date"), lit("")),
            coalesce(col("total_amount").cast("string"), lit(""))), 256))
    
    # Add silver metadata
    .withColumn("_silver_processed_timestamp", current_timestamp())
    .withColumn("_environment", lit(environment))
)

print("✓ Data quality transformations applied")

# COMMAND ----------

# DBTITLE 1,Deduplicate Records
# Deduplicate based on row_hash (keep most recent)
window_spec = Window.partitionBy("row_hash").orderBy(col("_silver_processed_timestamp").desc())

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
print(f"✓ Deduplication complete")

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
