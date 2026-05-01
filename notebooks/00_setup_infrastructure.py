# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Unity Catalog Setup for Invoice OCR Platform
# MAGIC
# MAGIC **Purpose**: Initialize Unity Catalog infrastructure (catalogs, schemas, volumes)
# MAGIC
# MAGIC **Run this once** before deploying the pipeline

# COMMAND ----------

# Configuration
dbutils.widgets.text("catalog_name", "invoice_analytics_dev", "Catalog Name")
dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "Environment")

catalog_name = dbutils.widgets.get("catalog_name")
environment = dbutils.widgets.get("environment")

print(f"Setting up Unity Catalog for: {environment}")
print(f"Catalog name: {catalog_name}")

# COMMAND ----------

# Create catalog
print(f"\nCreating catalog: {catalog_name}")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")
    print(f"✓ Catalog '{catalog_name}' ready")
except Exception as e:
    print(f"Note: {e}")
    print("(This is normal if catalog exists or you lack permissions)")

# COMMAND ----------

# Create schemas
schemas = ["bronze", "silver", "gold"]

print(f"\nCreating schemas...")
for schema in schemas:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
        print(f"✓ Schema '{schema}' ready")
    except Exception as e:
        print(f"⚠️  Error creating schema '{schema}': {e}")

# COMMAND ----------

# Create volumes for bronze layer
print(f"\nCreating volumes...")

volumes = [
    ("bronze", "raw_data", "Raw invoice CSV files from Kaggle dataset"),
    ("bronze", "checkpoints", "Auto Loader and streaming checkpoints")
]

for schema, volume, comment in volumes:
    try:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema}.{volume}
            COMMENT '{comment}'
        """)
        print(f"✓ Volume '{schema}.{volume}' ready")
        print(f"   Path: /Volumes/{catalog_name}/{schema}/{volume}/")
    except Exception as e:
        print(f"⚠️  Error creating volume '{schema}.{volume}': {e}")

# COMMAND ----------

# Verify setup
print("\n" + "="*80)
print("UNITY CATALOG SETUP SUMMARY")
print("="*80)

print(f"\n📁 Catalog: {catalog_name}")

print(f"\n📂 Schemas:")
schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
display(schemas_df)

print(f"\n💾 Volumes:")
for schema in ["bronze"]:
    try:
        volumes_df = spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema}")
        print(f"\nIn {schema}:")
        display(volumes_df)
    except:
        print(f"No volumes in {schema} yet")

print(f"\n✅ Setup complete! Ready to deploy pipeline.")
print(f"\nNext steps:")
print(f"1. Upload invoice CSV files to: /Volumes/{catalog_name}/bronze/raw_data/")
print(f"2. Deploy bundle: databricks bundle deploy -t {environment}")
print(f"3. Run pipeline: databricks bundle run -t {environment} invoice_data_pipeline")

# COMMAND ----------

# DBTITLE 1,Extract ZIP File (Run After Upload)
# Extract archive.zip and prepare CSV files
import zipfile
import os
import shutil

zip_file_path = f"/dbfs/Volumes/{catalog_name}/bronze/raw_data/archive.zip"
volume_path = f"/dbfs/Volumes/{catalog_name}/bronze/raw_data/"

print("Checking for archive.zip in volume...\n")
print(f"Looking at: {zip_file_path}")

if os.path.exists(zip_file_path):
    print("✓ Found archive.zip\n")
    
    # Extract ZIP file
    print("Extracting files...")
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(volume_path)
    print("✓ Files extracted\n")
    
    # Find CSV files in nested batch_1/batch_1/ structure
    batch_path = os.path.join(volume_path, "batch_1/batch_1/")
    
    if os.path.exists(batch_path):
        print(f"Moving CSV files from {batch_path}...")
        csv_files = [f for f in os.listdir(batch_path) if f.endswith('.csv')]
        
        for csv_file in csv_files:
            src = os.path.join(batch_path, csv_file)
            dst = os.path.join(volume_path, csv_file)
            shutil.move(src, dst)
            file_size = os.path.getsize(dst) / (1024 * 1024)
            print(f"  ✓ {csv_file} ({file_size:.2f} MB)")
        
        # Clean up nested folders
        shutil.rmtree(os.path.join(volume_path, "batch_1"))
        print("\n✓ Cleaned up nested folders")
    
    # Remove ZIP file
    os.remove(zip_file_path)
    print("✓ Removed archive.zip\n")
    
    # List final files
    print("="*80)
    print("FILES IN VOLUME:")
    print("="*80)
    files = [f for f in os.listdir(volume_path) if f.endswith('.csv')]
    for f in files:
        size = os.path.getsize(os.path.join(volume_path, f)) / (1024 * 1024)
        print(f"  • {f} ({size:.2f} MB)")
    
    print(f"\n✅ Ready! {len(files)} CSV files prepared for ingestion.")
    print(f"\nNow run: databricks bundle run -t {environment} invoice_data_pipeline_job")
else:
    print("❌ archive.zip not found!")
    print(f"\nPlease upload archive.zip to:")
    print(f"  /Volumes/{catalog_name}/bronze/raw_data/")
    print(f"\nSteps:")
    print(f"  1. Go to Catalog UI")
    print(f"  2. Navigate: {catalog_name} → bronze → raw_data")
    print(f"  3. Click 'Upload files'")
    print(f"  4. Select archive.zip from your local machine")
    print(f"  5. Come back and run this cell again")
