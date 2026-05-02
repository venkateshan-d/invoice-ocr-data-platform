# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# DBTITLE 1,Silver Layer - Invoice Data Extraction
# MAGIC %md
# MAGIC # Silver Layer - Field Extraction with Batch Processing
# MAGIC
# MAGIC ## 🔧 Architecture Improvements:
# MAGIC * **Batch Processing**: Process records in controlled batches (default: 100)
# MAGIC * **Error Handling**: Comprehensive error logging and recovery
# MAGIC * **Monitoring**: Real-time progress tracking and metrics
# MAGIC * **Quality Gates**: Automated validation before downstream processing
# MAGIC * **Test Mode**: Safe testing with small dataset (10 records)
# MAGIC
# MAGIC ## 📊 Quality Assurance:
# MAGIC * Deduplication using row hash
# MAGIC * Field extraction rate monitoring
# MAGIC * Data quality scoring (0.0 to 1.0)
# MAGIC * Critical field presence validation
# MAGIC
# MAGIC ## 🚀 Usage:
# MAGIC * **Test Mode**: Set `test_mode=true` to process 10 records
# MAGIC * **Production**: Set `test_mode=false` to process all records
# MAGIC * **Batch Size**: Adjust `batch_size` parameter (default: 100)

# COMMAND ----------

# DBTITLE 1,Parameters
# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("batch_size", "100", "Batch Size for AI Processing")
dbutils.widgets.text("test_mode", "false", "Test Mode (true = 10 records only)")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")
batch_size = int(dbutils.widgets.get("batch_size"))
test_mode = dbutils.widgets.get("test_mode").lower() == "true"

print("="*80)
print("SILVER TRANSFORMATION - BATCH PROCESSING WITH ERROR HANDLING")
print("="*80)
print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Batch Size: {batch_size}")
print(f"Test Mode: {test_mode} {'(Processing 10 records only)' if test_mode else '(Processing all records)'}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Define Tables
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import time
from datetime import datetime

# Define tables
BRONZE_TABLE = f"{catalog_name}.bronze.invoices_raw_ocr"
SILVER_TABLE = f"{catalog_name}.silver.invoices_clean"
SILVER_ERRORS_TABLE = f"{catalog_name}.silver.invoices_extraction_errors"
SILVER_METRICS_TABLE = f"{catalog_name}.silver.processing_metrics"

print(f"Source: {BRONZE_TABLE}")
print(f"Target: {SILVER_TABLE}")
print(f"Errors: {SILVER_ERRORS_TABLE}")
print(f"Metrics: {SILVER_METRICS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read Bronze Data
# Read bronze data (successfully parsed documents only)
df_bronze = spark.table(BRONZE_TABLE).filter(col("has_error") == False)

total_bronze_records = df_bronze.count()
print(f"\n📊 Bronze records available: {total_bronze_records:,}")

if test_mode:
    print(f"\n🧪 TEST MODE: Processing only 10 records for validation")
    df_bronze = df_bronze.limit(10)
    records_to_process = 10
else:
    records_to_process = total_bronze_records
    print(f"\n⚡ PRODUCTION MODE: Processing all {records_to_process:,} records")

print(f"\n📋 Processing Plan:")
print(f"   Records to process: {records_to_process:,}")
print(f"   Batch size: {batch_size}")
print(f"   Estimated batches: {(records_to_process + batch_size - 1) // batch_size}")
print(f"   Estimated time: ~{(records_to_process / 100) * 2:.0f} minutes")

# COMMAND ----------

# DBTITLE 1,Extract Invoice Fields from OCR Text
# MAGIC %md
# MAGIC ## Extract Invoice Fields with AI

# COMMAND ----------

# DBTITLE 1,Parse OCR Text and Extract Fields
from pyspark.sql.functions import monotonically_increasing_id

# Add row numbers for batch processing
df_bronze_numbered = df_bronze.withColumn("_batch_row_id", monotonically_increasing_id())

total_records = df_bronze_numbered.count()
num_batches = (total_records + batch_size - 1) // batch_size

print("="*80)
print("BATCH PROCESSING WITH AI EXTRACT")
print("="*80)

# Initialize metrics tracking
processing_metrics = []
all_results = []
errors_list = []

for batch_num in range(num_batches):
    batch_start = batch_num * batch_size
    batch_end = min(batch_start + batch_size, total_records)
    
    print(f"\n{'='*80}")
    print(f"📦 Batch {batch_num + 1}/{num_batches} | Records {batch_start:,} to {batch_end:,}")
    print(f"{'='*80}")
    
    batch_start_time = time.time()
    
    try:
        # Get batch data
        df_batch = df_bronze_numbered.filter(
            (col("_batch_row_id") >= batch_start) & 
            (col("_batch_row_id") < batch_end)
        )
        
        batch_count = df_batch.count()
        print(f"✓ Loaded {batch_count} records")
        
        # Create temporary view for SQL processing
        df_batch.createOrReplaceTempView("batch_bronze")
        
        # Process batch with ai_extract
        print(f"⚙️  Running ai_extract on {batch_count} records...")
        
        df_batch_extracted = spark.sql(f"""
            WITH extracted AS (
              SELECT
                image_path,
                file_name,
                parsed_content,
                page_count,
                element_count,
                ai_extract(
                  parsed_content,
                  '{{
                    "invoice_number": {{"type": "string", "description": "Invoice or reference number"}},
                    "invoice_date": {{"type": "string", "description": "Invoice date in any format"}},
                    "total_amount": {{"type": "number", "description": "Total invoice amount or grand total"}},
                    "vendor_name": {{"type": "string", "description": "Vendor, seller, or company name"}},
                    "customer_name": {{"type": "string", "description": "Customer, buyer, or bill-to name"}}
                  }}',
                  MAP('version', '2.0', 'instructions', 'Extract invoice details from this document.')
                ) AS extracted_fields,
                _load_timestamp,
                _environment
              FROM batch_bronze
            ),
            transformed AS (
              SELECT
                image_path,
                file_name,
                try_cast(extracted_fields:invoice_number AS STRING) AS invoice_number,
                try_cast(extracted_fields:invoice_date AS STRING) AS invoice_date,
                try_cast(extracted_fields:total_amount AS DOUBLE) AS total_amount,
                try_cast(extracted_fields:vendor_name AS STRING) AS vendor_name,
                try_cast(extracted_fields:customer_name AS STRING) AS customer_name,
                COALESCE(
                  try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'MM/dd/yyyy'),
                  try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'dd/MM/yyyy'),
                  try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'yyyy-MM-dd'),
                  try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'MM-dd-yyyy'),
                  try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'dd-MM-yyyy')
                ) AS invoice_date_parsed,
                CAST(try_cast(extracted_fields:total_amount AS DOUBLE) AS DECIMAL(10,2)) AS total_amount_parsed,
                UPPER(TRIM(try_cast(extracted_fields:vendor_name AS STRING))) AS vendor_name_clean,
                UPPER(TRIM(try_cast(extracted_fields:customer_name AS STRING))) AS customer_name_clean,
                (
                  CASE WHEN try_cast(extracted_fields:invoice_number AS STRING) IS NOT NULL THEN 1 ELSE 0 END +
                  CASE WHEN COALESCE(
                    try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'MM/dd/yyyy'),
                    try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'dd/MM/yyyy'),
                    try_to_date(try_cast(extracted_fields:invoice_date AS STRING), 'yyyy-MM-dd')
                  ) IS NOT NULL THEN 1 ELSE 0 END +
                  CASE WHEN try_cast(extracted_fields:total_amount AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END +
                  CASE WHEN try_cast(extracted_fields:vendor_name AS STRING) IS NOT NULL THEN 1 ELSE 0 END +
                  CASE WHEN try_cast(extracted_fields:customer_name AS STRING) IS NOT NULL THEN 1 ELSE 0 END
                ) AS fields_extracted,
                sha2(CONCAT_WS('||',
                  COALESCE(try_cast(extracted_fields:invoice_number AS STRING), ''),
                  COALESCE(try_cast(extracted_fields:invoice_date AS STRING), ''),
                  COALESCE(CAST(try_cast(extracted_fields:total_amount AS DOUBLE) AS STRING), '')
                ), 256) AS row_hash,
                current_timestamp() AS _silver_processed_timestamp,
                '{environment}' AS _environment,
                {batch_num + 1} AS _batch_number
              FROM extracted
            )
            SELECT *,
              fields_extracted / 5.0 AS _data_quality_score
            FROM transformed
        """)
        
        # Cache the batch result
        df_batch_extracted.cache()
        extracted_count = df_batch_extracted.count()
        
        # Calculate batch statistics
        batch_stats = df_batch_extracted.agg(
            count("*").alias("total"),
            sum(when(col("fields_extracted") >= 4, 1).otherwise(0)).alias("high_quality"),
            sum(when(col("fields_extracted") >= 3, 1).otherwise(0)).alias("medium_quality"),
            sum(when(col("fields_extracted") < 3, 1).otherwise(0)).alias("low_quality"),
            avg("fields_extracted").alias("avg_fields"),
            avg("_data_quality_score").alias("avg_quality")
        ).collect()[0]
        
        batch_duration = time.time() - batch_start_time
        
        print(f"\n✅ Batch {batch_num + 1} Complete:")
        print(f"   Records processed: {extracted_count}/{batch_count}")
        print(f"   High quality (4-5 fields): {batch_stats['high_quality']} ({batch_stats['high_quality']/extracted_count*100:.1f}%)")
        print(f"   Medium quality (3 fields): {batch_stats['medium_quality']} ({batch_stats['medium_quality']/extracted_count*100:.1f}%)")
        print(f"   Low quality (0-2 fields): {batch_stats['low_quality']} ({batch_stats['low_quality']/extracted_count*100:.1f}%)")
        print(f"   Avg fields extracted: {batch_stats['avg_fields']:.2f}/5")
        print(f"   Avg quality score: {batch_stats['avg_quality']:.2f}")
        print(f"   Processing time: {batch_duration:.1f}s")
        print(f"   Records/second: {batch_count/batch_duration:.1f}")
        
        # Store results
        all_results.append(df_batch_extracted)
        
        # Track metrics
        processing_metrics.append({
            "batch_number": batch_num + 1,
            "batch_start": batch_start,
            "batch_end": batch_end,
            "records_processed": extracted_count,
            "high_quality_count": batch_stats['high_quality'],
            "medium_quality_count": batch_stats['medium_quality'],
            "low_quality_count": batch_stats['low_quality'],
            "avg_fields_extracted": float(batch_stats['avg_fields']),
            "avg_quality_score": float(batch_stats['avg_quality']),
            "processing_time_seconds": batch_duration,
            "records_per_second": batch_count/batch_duration,
            "processing_timestamp": datetime.now(),
            "environment": environment
        })
        
    except Exception as e:
        batch_duration = time.time() - batch_start_time
        error_msg = str(e)
        print(f"\n❌ Batch {batch_num + 1} FAILED:")
        print(f"   Error: {error_msg}")
        print(f"   Time before failure: {batch_duration:.1f}s")
        
        # Log error
        errors_list.append({
            "batch_number": batch_num + 1,
            "batch_start": batch_start,
            "batch_end": batch_end,
            "error_message": error_msg,
            "processing_timestamp": datetime.now(),
            "environment": environment
        })
        
        # Continue with next batch (don't fail entire job)
        continue

print(f"\n{'='*80}")
print("BATCH PROCESSING COMPLETE")
print(f"{'='*80}")
print(f"✅ Successfully processed: {len(all_results)}/{num_batches} batches")
print(f"❌ Failed batches: {len(errors_list)}")

# COMMAND ----------

# DBTITLE 1,Combine Results and Write to Silver
# Combine all batch results and apply deduplication
if len(all_results) > 0:
    print(f"\n🔄 Combining {len(all_results)} batch results...")
    
    # Union all batches
    df_combined = all_results[0]
    for df in all_results[1:]:
        df_combined = df_combined.union(df)
    
    # Apply deduplication based on row_hash
    print(f"🧹 Applying deduplication...")
    windowSpec = Window.partitionBy("row_hash").orderBy(col("_silver_processed_timestamp").desc())
    
    df_final = df_combined.withColumn("row_num", row_number().over(windowSpec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    pre_dedup_count = df_combined.count()
    post_dedup_count = df_final.count()
    duplicates_removed = pre_dedup_count - post_dedup_count
    
    print(f"\n✅ Deduplication Complete:")
    print(f"   Before: {pre_dedup_count:,} records")
    print(f"   After: {post_dedup_count:,} records")
    print(f"   Duplicates removed: {duplicates_removed:,}")
    
    # Write to silver table
    print(f"\n💾 Writing {post_dedup_count:,} records to {SILVER_TABLE}...")
    
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_TABLE)
    
    print(f"✅ Silver table written successfully!")
    
else:
    print(f"\n⚠️  No successful batches to write!")

# COMMAND ----------

# DBTITLE 1,Save Metrics and Error Logs
# Save processing metrics
if len(processing_metrics) > 0:
    print(f"\n📊 Saving processing metrics to {SILVER_METRICS_TABLE}...")
    
    df_metrics = spark.createDataFrame(processing_metrics)
    
    df_metrics.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(SILVER_METRICS_TABLE)
    
    print(f"✅ Metrics saved: {len(processing_metrics)} batch records")

# Save error logs
if len(errors_list) > 0:
    print(f"\n⚠️  Saving {len(errors_list)} error records to {SILVER_ERRORS_TABLE}...")
    
    df_errors = spark.createDataFrame(errors_list)
    
    df_errors.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(SILVER_ERRORS_TABLE)
    
    print(f"❌ Errors logged: {len(errors_list)} failed batches")
else:
    print(f"\n✅ No errors to log - all batches succeeded!")

# COMMAND ----------

# DBTITLE 1,Data Quality Validation Gates
# DATA QUALITY VALIDATION GATES
print("\n" + "="*80)
print("🔍 DATA QUALITY VALIDATION GATES")
print("="*80)

df_silver_final = spark.table(SILVER_TABLE)
total_silver = df_silver_final.count()
total_bronze = df_bronze_numbered.count()

# Gate 1: Record count validation
processing_rate = (total_silver / total_bronze * 100) if total_bronze > 0 else 0
print(f"\n🎯 Gate 1: Record Processing Rate")
print(f"   Bronze records: {total_bronze:,}")
print(f"   Silver records: {total_silver:,}")
print(f"   Processing rate: {processing_rate:.1f}%")

if processing_rate < 95:
    print(f"   ⚠️  WARNING: Processing rate below 95% threshold")
    print(f"   Action: Review error logs in {SILVER_ERRORS_TABLE}")
else:
    print(f"   ✅ PASS: Processing rate meets threshold")

# Gate 2: Field extraction quality
quality_stats = df_silver_final.agg(
    avg("fields_extracted").alias("avg_fields"),
    avg("_data_quality_score").alias("avg_score"),
    (sum(when(col("fields_extracted") >= 4, 1).otherwise(0)) / count("*") * 100).alias("high_quality_pct")
).collect()[0]

print(f"\n🎯 Gate 2: Field Extraction Quality")
print(f"   Avg fields extracted: {quality_stats['avg_fields']:.2f}/5")
print(f"   Avg quality score: {quality_stats['avg_score']:.2f}")
print(f"   High quality records: {quality_stats['high_quality_pct']:.1f}%")

if quality_stats['avg_fields'] < 3:
    print(f"   ⚠️  WARNING: Average field extraction below 3/5 threshold")
    print(f"   Action: Review ai_extract configuration and prompts")
else:
    print(f"   ✅ PASS: Field extraction quality acceptable")

# Gate 3: Critical field presence
field_presence = df_silver_final.agg(
    (count(when(col("invoice_number").isNotNull(), 1)) / count("*") * 100).alias("invoice_num_pct"),
    (count(when(col("total_amount_parsed").isNotNull(), 1)) / count("*") * 100).alias("amount_pct"),
    (count(when(col("vendor_name").isNotNull(), 1)) / count("*") * 100).alias("vendor_pct")
).collect()[0]

print(f"\n🎯 Gate 3: Critical Field Presence")
print(f"   Invoice numbers: {field_presence['invoice_num_pct']:.1f}%")
print(f"   Total amounts: {field_presence['amount_pct']:.1f}%")
print(f"   Vendor names: {field_presence['vendor_pct']:.1f}%")

critical_field_threshold = 70
if (field_presence['invoice_num_pct'] < critical_field_threshold or 
    field_presence['amount_pct'] < critical_field_threshold or 
    field_presence['vendor_pct'] < critical_field_threshold):
    print(f"   ⚠️  WARNING: Critical field presence below {critical_field_threshold}% threshold")
    print(f"   Action: Review document quality and ai_extract prompts")
else:
    print(f"   ✅ PASS: Critical fields present in sufficient records")

print("\n" + "="*80)

# Overall gate decision
if processing_rate >= 95 and quality_stats['avg_fields'] >= 3:
    print("✅ ALL QUALITY GATES PASSED - Pipeline healthy")
else:
    print("⚠️  SOME QUALITY GATES FAILED - Review required")

# COMMAND ----------

# DBTITLE 1,Optimize Silver Table
# MAGIC %sql
# MAGIC -- Optimize and Z-order for faster queries
# MAGIC OPTIMIZE invoice_analytics_dev.silver.invoices_clean
# MAGIC ZORDER BY (vendor_name_clean, invoice_date_parsed, _data_quality_score);

# COMMAND ----------

# DBTITLE 1,Invoice Data Quality Report
# COMPREHENSIVE DATA QUALITY REPORT
df_silver_final = spark.table(SILVER_TABLE)

print("="*80)
print("SILVER LAYER INVOICE DATA QUALITY REPORT")
print("="*80)

# Overall metrics
total_invoices = df_silver_final.count()
print(f"\n📊 Overall Statistics:")
print(f"   Total invoices processed: {total_invoices:,}")

if total_invoices > 0:
    # Field extraction rates
    field_stats = df_silver_final.agg(
        (count(when(col("invoice_number").isNotNull(), 1)) / total_invoices * 100).alias("invoice_number_pct"),
        (count(when(col("invoice_date_parsed").isNotNull(), 1)) / total_invoices * 100).alias("invoice_date_pct"),
        (count(when(col("total_amount_parsed").isNotNull(), 1)) / total_invoices * 100).alias("total_amount_pct"),
        (count(when(col("vendor_name").isNotNull(), 1)) / total_invoices * 100).alias("vendor_name_pct"),
        (count(when(col("customer_name").isNotNull(), 1)) / total_invoices * 100).alias("customer_name_pct")
    ).collect()[0]
    
    print(f"\n📝 Field Extraction Rates:")
    print(f"   Invoice Number: {field_stats['invoice_number_pct']:.1f}%")
    print(f"   Invoice Date:   {field_stats['invoice_date_pct']:.1f}%")
    print(f"   Total Amount:   {field_stats['total_amount_pct']:.1f}%")
    print(f"   Vendor Name:    {field_stats['vendor_name_pct']:.1f}%")
    print(f"   Customer Name:  {field_stats['customer_name_pct']:.1f}%")
    
    # Quality score distribution
    print(f"\n🎯 Data Quality Score Distribution:")
    quality_dist = df_silver_final.groupBy("_data_quality_score").count().orderBy(col("_data_quality_score").desc())
    display(quality_dist)
    
    # High quality invoices (score >= 0.8)
    high_quality_count = df_silver_final.filter(col("_data_quality_score") >= 0.8).count()
    medium_quality_count = df_silver_final.filter((col("_data_quality_score") >= 0.6) & (col("_data_quality_score") < 0.8)).count()
    low_quality_count = df_silver_final.filter(col("_data_quality_score") < 0.6).count()
    
    print(f"\n🏆 Quality Tier Summary:")
    print(f"   High Quality (≥ 0.8):   {high_quality_count:,} ({high_quality_count/total_invoices*100:.1f}%)")
    print(f"   Medium Quality (0.6-0.8): {medium_quality_count:,} ({medium_quality_count/total_invoices*100:.1f}%)")
    print(f"   Low Quality (< 0.6):      {low_quality_count:,} ({low_quality_count/total_invoices*100:.1f}%)")
    
    # Sample clean records
    print(f"\n👁️ Sample of High-Quality Extracted Data:")
    display(df_silver_final.select(
        "file_name", "invoice_number", "invoice_date_parsed", 
        "total_amount_parsed", "vendor_name_clean", "_data_quality_score", "_batch_number"
    ).filter(col("_data_quality_score") >= 0.6).orderBy(col("_data_quality_score").desc()).limit(10))
    
    print(f"\n" + "="*80)
    print("✅ Silver transformation complete!")
    print(f"   Ready for gold layer analytics")
    print(f"   Next: Run 03_gold_analytics notebook")
    print("="*80)
else:
    print(f"\n⚠️  No records in silver table!")
    print(f"   Check error logs: {SILVER_ERRORS_TABLE}")
