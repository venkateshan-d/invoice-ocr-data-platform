# Workflows Directory

This directory contains job workflow definitions for the Invoice OCR Data Platform.

## Job Definitions

All job configurations are stored here as `.job.yml` files:

```
workflows/
└── invoice_pipeline.job.yml  # Main ETL pipeline job
```

## Job Structure

Each job file defines:
- **git_source**: Repository and branch for notebook code
- **job_clusters**: Compute configuration
- **tasks**: Pipeline tasks with dependencies
- **schedule**: Execution schedule
- **notifications**: Email alerts

## Available Jobs

### invoice_data_pipeline_job

**Description**: End-to-end medallion pipeline (Bronze → Silver → Gold)

**Tasks**:
1. `bronze_ingestion` - Ingest raw CSV files using Auto Loader
2. `silver_transformation` - Clean, validate, and deduplicate data
3. `gold_analytics` - Create business aggregations

**Schedule**: Daily at 2 AM Pacific (paused by default in dev)

**To Run**:
```bash
# Dev environment
databricks bundle run -t dev invoice_data_pipeline_job

# Prod environment
databricks bundle run -t prod invoice_data_pipeline_job
```

## Adding New Jobs

1. Create a new file: `workflows/your_job_name.job.yml`
2. Follow this structure:

```yaml
resources:
  jobs:
    your_job_name:
      name: "[${var.environment}] Your Job Name"
      
      git_source:
        git_url: https://github.com/YOUR_USERNAME/invoice-ocr-data-platform
        git_provider: github
        git_branch: main
      
      job_clusters:
        - job_cluster_key: main_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
      
      tasks:
        - task_key: your_task
          job_cluster_key: main_cluster
          notebook_task:
            notebook_path: notebooks/your_notebook
            source: GIT
```

3. Deploy: `databricks bundle deploy -t dev`

## Configuration

Job configurations can be overridden per environment in `config/dev.yml` or `config/prod.yml`:

```yaml
resources:
  jobs:
    invoice_data_pipeline_job:
      max_retries: 3
      timeout_seconds: 7200
```

## Monitoring

View job runs:
```bash
# List recent runs
databricks jobs list-runs --limit 10

# Get specific run
databricks jobs get-run --run-id <run-id>
```

## Troubleshooting

### Job not found after deploy
**Solution**: Verify `databricks.yml` includes workflows:
```yaml
include:
  - workflows/*.yml
```

### Notebook not found
**Solution**: Check notebook paths are relative without extension:
- ✅ `notebooks/01_bronze_ingestion`
- ❌ `./notebooks/01_bronze_ingestion.py`

### Git source error
**Solution**: Update `git_url` in the job file with your actual GitHub repository URL

## Related Documentation

- [GIT_DEPLOYMENT.md](../GIT_DEPLOYMENT.md) - Complete deployment guide
- [README.md](../README.md) - Project overview
- [CHANGELOG.md](../CHANGELOG.md) - Recent changes
