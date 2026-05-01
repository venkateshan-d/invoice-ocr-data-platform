# Workflows

This directory contains workflow-related documentation and configurations for the Invoice OCR Data Platform.

## Available Workflows

### Invoice Data Pipeline

**Name**: `invoice_data_pipeline`

**Description**: End-to-end medallion pipeline processing invoice data from raw CSV to analytics-ready tables.

**Tasks**:
1. **bronze_ingestion**: Ingest raw CSV files using Auto Loader
2. **silver_transformation**: Clean, validate, and deduplicate data
3. **gold_analytics**: Create business aggregations and metrics

**Schedule**: Daily at 2 AM Pacific Time

**Trigger Manually**:
```bash
# Dev environment
databricks bundle run -t dev invoice_data_pipeline

# Prod environment
databricks bundle run -t prod invoice_data_pipeline
```

## Task Dependencies

```
bronze_ingestion
       ↓
silver_transformation
       ↓
gold_analytics
```

Each task must complete successfully before the next one starts.

## Monitoring

### Check Job Status

```bash
# List all runs
databricks jobs list-runs --job-id <job-id>

# Get run details
databricks jobs get-run --run-id <run-id>
```

### View Logs

Access logs in Databricks UI:
1. Go to **Workflows** → **Jobs**
2. Find your job (e.g., "[DEV] Invoice Data Pipeline")
3. Click on a run to view task logs

## Customization

To modify the workflow:
1. Edit `databricks.yml` in the project root
2. Adjust task parameters, cluster configuration, or schedule
3. Redeploy: `databricks bundle deploy -t <env>`

## Adding New Tasks

Add new tasks to `databricks.yml` under `resources.jobs.invoice_data_pipeline.tasks`:

```yaml
- task_key: new_task_name
  depends_on:
    - task_key: silver_transformation
  job_cluster_key: main_cluster
  notebook_task:
    notebook_path: ./notebooks/04_new_task
    base_parameters:
      catalog_name: ${var.catalog_name}
      environment: ${var.environment}
```

## Troubleshooting

### Task Failures

1. Check task logs in Databricks UI
2. Verify input data exists
3. Check Unity Catalog permissions
4. Validate configuration in `config/` directory

### Retry Failed Runs

Failed tasks will auto-retry based on environment settings:
- **Dev**: 1 retry
- **Prod**: 3 retries

Manual retry:
```bash
databricks jobs run-now --job-id <job-id>
```
