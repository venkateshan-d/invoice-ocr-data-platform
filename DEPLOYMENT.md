# Deployment Guide - Invoice OCR Data Platform

This guide walks you through deploying the Invoice OCR Data Platform using Databricks Asset Bundles.

## Prerequisites Checklist

- [ ] Databricks workspace with Unity Catalog enabled
- [ ] Databricks CLI installed: `pip install databricks-cli`
- [ ] Git repository initialized (if pushing to GitHub)
- [ ] Kaggle account and API credentials

## Step 1: Configure Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure with your workspace
databricks configure --token

# You'll be prompted for:
# - Databricks Host: https://your-workspace.cloud.databricks.com
# - Token: (generate from User Settings → Access Tokens)
```

## Step 2: Validate Bundle Configuration

```bash
# Navigate to project directory
cd /path/to/invoice-ocr-data-platform

# Validate the bundle configuration
databricks bundle validate -t dev
```

Expected output:
```
✓ Configuration is valid
```

## Step 3: Initialize Unity Catalog

**Option A: Via Notebook (Recommended)**

1. Open the workspace in Databricks UI
2. Navigate to `notebooks/00_setup_infrastructure.py`
3. Attach to a cluster (or use serverless)
4. Set parameters:
   - `catalog_name`: invoice_analytics_dev (for dev)
   - `environment`: dev
5. Run all cells

**Option B: Via SQL Editor**

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS invoice_analytics_dev;
USE CATALOG invoice_analytics_dev;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create volumes
CREATE VOLUME IF NOT EXISTS bronze.raw_data;
CREATE VOLUME IF NOT EXISTS bronze.checkpoints;
```

## Step 4: Upload Invoice Data

1. Navigate to: **Catalog** → `invoice_analytics_dev` → `bronze` → `raw_data`
2. Click **"Upload files"**
3. Upload your CSV files:
   - `batch1_1.csv`
   - `batch1_2.csv`
   - `batch1_3.csv`

Or download from Kaggle:
```bash
kaggle datasets download -d osamahosamabdellatif/high-quality-invoice-images-for-ocr
unzip high-quality-invoice-images-for-ocr.zip
# Upload via Databricks UI
```

## Step 5: Deploy to Development

```bash
# Deploy the bundle to dev environment
databricks bundle deploy -t dev
```

This will:
- ✅ Upload notebooks to workspace
- ✅ Create the job workflow
- ✅ Configure clusters and schedules
- ✅ Set up notifications

Expected output:
```
Uploading bundle files...
Deploying resources...
✓ Job created: [DEV] Invoice Data Pipeline
Deployment complete!
```

## Step 6: Run the Pipeline

**Option A: Via CLI**

```bash
# Trigger the pipeline
databricks bundle run -t dev invoice_data_pipeline

# Monitor the run
databricks jobs list-runs --job-id <job-id>
```

**Option B: Via Databricks UI**

1. Go to **Workflows** → **Jobs**
2. Find **"[DEV] Invoice Data Pipeline"**
3. Click **"Run now"**
4. Monitor task progress

## Step 7: Verify Results

Check the data in each layer:

```sql
-- Bronze layer
SELECT COUNT(*) as bronze_count 
FROM invoice_analytics_dev.bronze.invoices_raw;

-- Silver layer
SELECT COUNT(*) as silver_count,
       AVG(_data_quality_score) as avg_quality
FROM invoice_analytics_dev.silver.invoices_clean;

-- Gold layer
SELECT * FROM invoice_analytics_dev.gold.invoice_summary;
SELECT * FROM invoice_analytics_dev.gold.data_quality_metrics;
```

## Step 8: Deploy to Production

Once dev testing is complete:

```bash
# Update prod catalog (if needed)
# Edit databricks.yml and config/prod.yml

# Run setup for prod
# Execute notebooks/00_setup_infrastructure.py with:
#   catalog_name: invoice_analytics
#   environment: prod

# Deploy to prod
databricks bundle deploy -t prod

# Run prod pipeline
databricks bundle run -t prod invoice_data_pipeline
```

## Troubleshooting

### Issue: "Catalog not found"
**Solution**: Run the setup notebook (Step 3) or create catalog manually

### Issue: "Volume path not found"
**Solution**: Verify volumes exist:
```sql
SHOW VOLUMES IN invoice_analytics_dev.bronze;
```

### Issue: "No files to process"
**Solution**: Check that CSV files are uploaded to the volume:
```python
dbutils.fs.ls("/Volumes/invoice_analytics_dev/bronze/raw_data/")
```

### Issue: "Permission denied"
**Solution**: Ensure you have:
- CREATE CATALOG permission
- USE CATALOG permission
- CREATE SCHEMA permission
- WRITE access to volumes

### Issue: "Bundle validation failed"
**Solution**: 
```bash
# Check YAML syntax
databricks bundle validate -t dev

# Common fixes:
# - Check indentation in YAML files
# - Verify all notebook paths exist
# - Ensure catalog names are consistent
```

## Monitoring & Maintenance

### View Pipeline Runs

```bash
# List recent runs
databricks jobs list-runs --limit 10

# Get run details
databricks jobs get-run --run-id <run-id>
```

### Check Data Quality

```sql
-- Quality score distribution
SELECT 
    _data_quality_score,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM invoice_analytics_dev.silver.invoices_clean
GROUP BY _data_quality_score
ORDER BY _data_quality_score DESC;
```

### Update Pipeline

```bash
# Make changes to notebooks or config

# Redeploy
databricks bundle deploy -t dev

# Changes take effect on next run
databricks bundle run -t dev invoice_data_pipeline
```

## Rollback Procedure

If a deployment fails:

```bash
# 1. Revert to previous bundle version
git checkout <previous-commit>

# 2. Redeploy
databricks bundle deploy -t dev

# 3. Verify
databricks bundle validate -t dev
```

## Production Deployment Checklist

Before deploying to prod:

- [ ] All tests pass in dev
- [ ] Data quality metrics are acceptable
- [ ] Pipeline runs end-to-end successfully
- [ ] Notebooks are documented
- [ ] Error handling is robust
- [ ] Monitoring alerts configured
- [ ] Backup procedures in place
- [ ] Stakeholders notified
- [ ] Deployment window scheduled
- [ ] Rollback plan prepared

## Support

For issues or questions:
1. Check the [README.md](README.md) for documentation
2. Review [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines
3. Check Databricks job logs for error details
4. Open an issue on GitHub

---

**Happy Deploying! 🚀**
