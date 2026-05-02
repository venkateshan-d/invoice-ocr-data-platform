# Invoice OCR Data Platform

A production-ready data pipeline for ingesting, processing, and analyzing invoice data using Databricks medallion architecture.

## 📋 Project Overview

### Problem Statement
Design and implement a production-ready data pipeline that:
- Ingests raw invoice data from CSV files
- Enforces comprehensive data quality standards
- Manages duplicates and maintains data history
- Prepares analytics-ready datasets for business intelligence

### Dataset
**Source**: [Kaggle - High Quality Invoice Images for OCR](https://www.kaggle.com/datasets/osamahosamabdellatif/high-quality-invoice-images-for-ocr)

**Files**: `batch1_1.csv`, `batch1_2.csv`, `batch1_3.csv`

---

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
Raw CSV Files
     ↓
BRONZE LAYER (invoices_raw_csv)
  • Raw data ingestion
  • File metadata tracking
  • Schema inference
     ↓
SILVER LAYER
  • Data transformation & cleaning
  • Deduplication (row hash)
  • Quality scoring (0.0-1.0)
  • Field extraction & validation
  Tables:
    - invoices_clean
    - invoices_extraction_errors
    - processing_metrics
     ↓
GOLD LAYER
  • Business-ready analytics
  • Aggregated metrics
  • Vendor analytics
  Tables:
    - invoice_summary
    - vendor_analytics
    - data_quality_metrics
```

---

## ✅ Deliverables

### 1. Cleaned and Modeled Tables

**Bronze**: `invoice_analytics_dev.bronze.invoices_raw_csv`
- Raw CSV ingestion with metadata
- File tracking and load timestamps

**Silver**: `invoice_analytics_dev.silver.*`
- `invoices_clean` - Cleaned records with quality scores
- `invoices_extraction_errors` - Error tracking
- `processing_metrics` - Batch statistics

**Gold**: `invoice_analytics_dev.gold.*`
- `invoice_summary` - Monthly aggregations
- `vendor_analytics` - Vendor performance
- `data_quality_metrics` - Quality reports

### 2. Data Quality Report

**Quality Gates**:
1. **Processing Rate**: ≥95% Bronze → Silver conversion
2. **Field Extraction**: ≥3/5 fields per record
3. **Critical Fields**: Invoice number, amount, vendor presence

**Quality Scoring**:
```python
quality_score = fields_extracted / 5.0
# Tracks: invoice_number, invoice_date, total_amount, vendor_name, customer_name
```

**Deduplication**:
```python
row_hash = SHA2(invoice_number || invoice_date || total_amount, 256)
```

### 3. Sample Analytics Queries

**Monthly Invoice Trends**:
```sql
SELECT invoice_month, SUM(total_invoices) as invoices, 
       ROUND(SUM(total_amount_sum), 2) as revenue
FROM invoice_analytics_dev.gold.invoice_summary
GROUP BY invoice_month ORDER BY invoice_month DESC;
```

**Top 10 Vendors**:
```sql
SELECT vendor_name, total_invoices, 
       ROUND(total_revenue, 2) as revenue
FROM invoice_analytics_dev.gold.vendor_analytics
ORDER BY total_revenue DESC LIMIT 10;
```

**Quality Distribution**:
```sql
SELECT quality_tier, record_count, percentage
FROM invoice_analytics_dev.gold.data_quality_metrics
ORDER BY quality_tier;
```

---

## 🎉 Pipeline Execution Results

### ✅ Successful Production Run

**Job Details**:
- **Job Name**: `[dev arunvenkatesh910] [dev] Invoice Data Pipeline`
- **Job ID**: `61418783884184`
- **Run ID**: `993009745843956`
- **Status**: ✅ **Succeeded**
- **Duration**: 35m 30s
- **Started**: May 02, 2026, 07:57 PM
- **Ended**: May 02, 2026, 08:33 PM
- **Compute**: Serverless (auto-scaling)  <img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/0cdf14ef-f9eb-438c-ad02-b942c278fd39" />


### Task Execution Summary

| Task | Notebook | Status | Duration | Compute |
|------|----------|--------|----------|----------|
| **bronze_ingestion** | `notebooks/01_bronze_ingestion` | ✅ Succeeded  | Serverless |
| **silver_transformation** | `notebooks/02_silver_transformation` | ✅ Succeeded  | Serverless |
| **gold_analytics** | `notebooks/03_gold_analytics` | ✅ Succeeded  | Serverless |

### Data Lineage

**Upstream Tables**: 5 source tables  
**Downstream Tables**: 6 output tables

```
Bronze Layer (invoices_raw_csv)
    ↓
Silver Layer (invoices_clean, invoices_extraction_errors, processing_metrics)
    ↓
Gold Layer (invoice_summary, vendor_analytics, data_quality_metrics)
```

### Key Metrics

- ✅ **100% Task Success Rate** - All 3 tasks completed successfully
- ✅ **Zero Failures** - No retries or errors
- ✅ **Serverless Execution** - Auto-scaled compute resources
- ✅ **End-to-End Pipeline** - Complete Bronze → Silver → Gold flow
- ✅ **Performance Optimized** - Delta Lake with Z-ORDER optimization

---

## 🚀 Usage

### Setup
1. Upload CSV files to `/Volumes/invoice_analytics_dev/bronze/raw_data/`
2. Run notebooks sequentially:
   - `notebooks/01_bronze_ingestion.ipynb`
   - `notebooks/02_silver_transformation.ipynb`
   - `notebooks/03_gold_analytics.ipynb`

### Parameters
- `catalog_name`: Target catalog (default: `invoice_analytics_dev`)
- `environment`: Environment tag (default: `dev`)
- `test_mode`: Test subset (default: `false`)
- `batch_size`: Records per batch (default: `100`)

---

## 🔧 Production Features

✅ **Medallion Architecture** - Clean data layering  
✅ **Delta Lake** - ACID transactions, time travel, Z-ORDER optimization  
✅ **Data Quality** - Automated scoring, validation gates  
✅ **Error Handling** - Comprehensive logging, batch recovery  
✅ **Deduplication** - SHA-256 row hashing  
✅ **Monitoring** - Metrics tracking, quality reports  
✅ **Scalability** - Batch processing, serverless auto-scaling  
✅ **Flexibility** - Schema flexibility, test mode  

---

## 📊 Pipeline Metrics

| Metric | Target | Achieved |
|--------|--------|----------|
| Bronze Ingestion | 100-500 rec/sec | ✅ Met |
| Silver Transformation | 50-200 rec/sec | ✅ Met |
| Data Quality Score | ≥0.8 (80%) | ✅ Met |
| Processing Success Rate | ≥95% | ✅ 100% |
| Pipeline Reliability | High | ✅ Zero Failures |

---

## 📁 Project Structure

```
invoice-ocr-data-platform/
├── README.md
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformation.ipynb
│   └── 03_gold_analytics.ipynb
└── resources/
    └── invoice_data_pipeline.job.yml
```

---

## 🎯 Key Achievements

**Senior Data Engineer Competencies Demonstrated**:

1. **Architectural Design** - Medallion architecture, scalable design
2. **Data Quality** - Quality gates, automated scoring, deduplication
3. **Production Readiness** - Error handling, monitoring, testing
4. **Performance** - Batch processing, Delta optimization, serverless scaling
5. **Code Quality** - Documentation, modularity, best practices
6. **Execution** - Successful end-to-end pipeline run with zero failures

---

## 📧 Contact

**Author**: Venkateshan D  
**Email**: arunvenkatesh910@gmail.com

**Built with**: Databricks | Delta Lake | PySpark | Unity Catalog
