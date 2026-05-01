# Invoice OCR Data Platform

Production-ready invoice data pipeline built with Databricks, implementing a Medallion architecture (Bronze → Silver → Gold) for OCR invoice data processing.

## 🏗️ Architecture

```
Bronze (Raw)     →     Silver (Clean)     →     Gold (Analytics)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Auto Loader              Data Quality            Business Aggregations
Schema Evolution         Deduplication           Summary Tables
Metadata Tracking        Validation              Analytics Views
```

## 📊 Dataset

**Source**: [Kaggle - High Quality Invoice Images for OCR](https://www.kaggle.com/datasets/osamahosamabdellatif/high-quality-invoice-images-for-ocr)

The pipeline processes invoice CSV data extracted from OCR processing.

## 🚀 Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI installed (`pip install databricks-cli`)
- Kaggle account and API credentials

### Deployment

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd invoice-ocr-data-platform

# 2. Configure Databricks CLI
databricks configure --token

# 3. Deploy to dev environment
databricks bundle deploy -t dev

# 4. Run the pipeline
databricks bundle run -t dev invoice_data_pipeline
```

### Deploy to Production

```bash
# Deploy to production
databricks bundle deploy -t prod

# Run production pipeline
databricks bundle run -t prod invoice_data_pipeline
```

## 📁 Project Structure

```
invoice-ocr-data-platform/
├── databricks.yml              # Bundle configuration
├── config/
│   ├── dev.yml                 # Dev environment settings
│   └── prod.yml                # Prod environment settings
├── notebooks/
│   ├── 01_bronze_ingestion.py  # Raw data ingestion
│   ├── 02_silver_transformation.py  # Data quality & cleaning
│   └── 03_gold_analytics.py    # Analytics aggregations
└── workflows/
    └── README.md               # Workflow documentation
```

## 🎯 Pipeline Stages

### 1. Bronze Layer (`bronze.invoices_raw`)
- **Purpose**: Ingest raw CSV files from Unity Catalog Volume
- **Technology**: Auto Loader with schema inference
- **Features**:
  - Incremental processing with checkpoints
  - Schema evolution support
  - Metadata tracking (_input_file, _load_timestamp, _rescue_data)
  - Corrupt record handling

### 2. Silver Layer (`silver.invoices_clean`)
- **Purpose**: Clean and validate invoice data
- **Transformations**:
  - Data type standardization
  - Duplicate removal (row-level deduplication)
  - Data quality scoring (0.0 - 1.0 scale)
  - Business rule validations
  - Null value flagging

### 3. Gold Layer (Analytics Tables)
- **Tables**:
  - `gold.invoice_summary`: Aggregated invoice metrics
  - `gold.data_quality_metrics`: Pipeline health monitoring
- **Purpose**: Business-ready analytics and reporting

## ⚙️ Configuration

### Environment Variables

The pipeline supports two environments with different configurations:

| Setting | Dev | Prod |
|---------|-----|------|
| Catalog | `invoice_analytics_dev` | `invoice_analytics` |
| Max Retries | 1 | 3 |
| Timeout | 1 hour | 2 hours |
| Max Null % | 20% | 5% |
| Duplicate Threshold | 10% | 2% |
| Batch Size | 1,000 | 10,000 |

### Modify Configurations

Edit files in `config/` directory:
- `config/dev.yml` - Development settings
- `config/prod.yml` - Production settings

## 📅 Scheduling

The pipeline runs daily at 2 AM Pacific Time (configurable in `databricks.yml`):

```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/Los_Angeles"
```

## 📧 Notifications

- **Dev**: Email on failure
- **Prod**: Email on both failure and success

Configure recipients in `databricks.yml` under `email_notifications`.

## 🔍 Monitoring

### Data Quality Metrics

Query the gold layer for pipeline health:

```sql
SELECT * FROM invoice_analytics.gold.data_quality_metrics
ORDER BY metric_date DESC;
```

### Pipeline Statistics

```sql
SELECT 
    environment,
    total_invoices,
    unique_invoices,
    avg_quality_score,
    latest_load_timestamp
FROM invoice_analytics.gold.invoice_summary;
```

## 🛠️ Development

### Local Testing

```python
# Run individual notebooks in Databricks workspace
# Set widget parameters:
dbutils.widgets.text("catalog_name", "invoice_analytics_dev")
dbutils.widgets.text("environment", "dev")
```

### Adding New Transformations

1. Modify notebooks in `notebooks/` directory
2. Test in dev environment
3. Deploy changes: `databricks bundle deploy -t dev`
4. Promote to prod: `databricks bundle deploy -t prod`

## 📝 Unity Catalog Structure

```
invoice_analytics (or invoice_analytics_dev)
├── bronze (schema)
│   ├── raw_data (volume)          # CSV file storage
│   ├── checkpoints (volume)       # Streaming checkpoints
│   └── invoices_raw (table)       # Raw ingested data
├── silver (schema)
│   └── invoices_clean (table)     # Cleaned & validated data
└── gold (schema)
    ├── invoice_summary (table)
    └── data_quality_metrics (table)
```

## 🔐 Security

- Credentials stored in Databricks Secrets (recommended)
- Unity Catalog for data governance
- Row-level and column-level security via UC policies

## 🤝 Contributing

1. Create a feature branch
2. Make changes and test in dev
3. Submit pull request
4. Deploy to prod after approval

## 📄 License

[Your License Here]

## 📧 Contact

[Your Contact Information]
