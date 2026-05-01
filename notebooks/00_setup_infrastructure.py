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
