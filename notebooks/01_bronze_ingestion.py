# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer - Invoice Data Ingestion
# MAGIC
# MAGIC **Purpose**: Ingest raw invoice CSV files from Unity Catalog Volume into Bronze Delta table
# MAGIC
# MAGIC **Features**:
# MAGIC - Auto Loader for incremental processing
# MAGIC - Schema inference and evolution
# MAGIC - Metadata tracking (_input_file, _load_timestamp, _rescue_data)
# MAGIC - Idempotent processing with checkpoints

# COMMAND ----------

# Get parameters from job or use defaults
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# Define paths
VOLUME_PATH = f"/Volumes/{catalog_name}/bronze/raw_data/"
CHECKPOINT_PATH = f"/Volumes/{catalog_name}/bronze/checkpoints/bronze_ingestion/"
BRONZE_TABLE = f"{catalog_name}.bronze.invoices_raw"

print(f"Source Path: {VOLUME_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print(f"Target Table: {BRONZE_TABLE}")

# COMMAND ----------

# Read CSV files with Auto Loader
df_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_rescue_data")
    .load(VOLUME_PATH)
    .withColumn("_input_file", input_file_name())
    .withColumn("_load_timestamp", current_timestamp())
    .withColumn("_environment", lit(environment))
)

print("✓ Auto Loader configured")
print(f"Schema inference location: {CHECKPOINT_PATH}schema")

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

# Verify ingestion
df_bronze_table = spark.table(BRONZE_TABLE)
row_count = df_bronze_table.count()
file_count = df_bronze_table.select("_input_file").distinct().count()

print("="*80)
print("BRONZE LAYER STATISTICS")
print("="*80)
print(f"Total records: {row_count:,}")
print(f"Source files processed: {file_count}")
print(f"Environment: {environment}")
print(f"Table: {BRONZE_TABLE}")

display(df_bronze_table.limit(10))

