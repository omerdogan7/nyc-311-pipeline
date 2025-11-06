# <img src="https://cdn.brandfetch.io/idSUrLOWbH/theme/light/logo.svg?c=1bxid64Mup7aczewSAYMX&t=1668081623507" alt="Databricks" width="300"/>

# NYC 311 Data Lakehouse Pipeline

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00ADD4?style=for-the-badge)](https://docs.databricks.com/data-governance/unity-catalog/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com)

Production-ready data lakehouse for NYC 311 Service Requests using **Databricks Unity Catalog**, **Lakeflow Declarative Pipelines (DLT)**, and **Medallion Architecture**.

---

## 📑 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Data Flow](#data-flow)
  - [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Quick Start (5 Minutes)](#quick-start-5-minutes)
  - [Detailed Deployment](#detailed-deployment)
- [Data Layers](#data-layers)
  - [Bronze Layer](#bronze-layer-raw-data-ingestion)
  - [Silver Layer](#silver-layer-data-quality--enrichment)
  - [Gold Layer](#gold-layer-analytics---dbt)
- [Configuration](#configuration)
  - [Unity Catalog Structure](#unity-catalog-structure)
  - [Environment Variables](#environment-variables)
  - [Pipeline Configuration](#pipeline-configuration)
- [Observability & Monitoring](#observability--monitoring)
- [Performance](#performance)
  - [Benchmarks](#performance-benchmarks)
- [Troubleshooting](#troubleshooting)
- [Development Workflow](#development-workflow)
- [Data Schema](#data-schema)
- [Security & Governance](#security--governance)
- [FAQ](#faq)
- [Resources](#resources)

---
## Overview

This project implements a **medallion architecture** (Bronze → Silver → Gold) for processing **NYC 311 service request data** (~42M records, 2010-2025) using:

- **Lakeflow Declarative Pipelines (DLT)** for orchestrated data pipelines
- **Unity Catalog** for centralized governance and data management
- **Delta Lake** for ACID transactions and time travel
- **dbt** for analytics transformations in the Gold layer
- **Auto Loader** for incremental S3 ingestion with schema evolution

**Key Features:**
- ✅ **Serverless compute** - Zero cluster management
- ✅ **99% data quality** - Borough recovery, date inference, validation flags
- ✅ **9-minute full pipeline** - Bronze → Silver processing (42M records)
- ✅ **Auto-compaction** - Optimized Delta file sizes
- ✅ **Schema evolution** - Handles new columns automatically
- ✅ **Partition pruning** - Year/month partitioning for fast queries

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│                 S3 Raw Data (External Volume)                    │
│                    s3://nyc-311-raw/                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                                │
│  • Auto Loader (Incremental Ingestion)                          │
│  • Schema Evolution (42→46 columns)                             │
│  • Metadata Tracking (_ingested_at, _source_file)               │
│                                                                   │
│  Table: nyc_311_dev.bronze.nyc_311_raw                           │
│  Size: 3.88 GB | Records: 42M | Files: 42                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                                │
│  • Borough Recovery (98.6% → 99.0%)                             │
│  • Closed Date Inference (DHS: 53,679 records)                  │
│  • Data Quality Flags (10+ validation rules)                    │
│  • Geographic Validation (NYC boundaries)                        │
│  • MERGE Upsert (SCD Type 1)                                     │
│                                                                   │
│  Table: nyc_311_dev.silver.silver_311                            │
│  Size: 3.61 GB | Records: 42M | Files: 191                       │
│  Partitions: created_year, created_month                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER (dbt)                           │
│  • Complaint Type Summaries                                      │
│  • Borough-Level Metrics                                         │
│  • Time-Series Aggregations                                      │
│  • Resolution Performance KPIs                                   │
│                                                                   │
│  Tables: nyc_311_dev.gold.* (dbt-managed)                        │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **External Volume**: Raw Parquet files in S3 (`s3://nyc-311-raw/`)
2. **Bronze Layer**: Auto Loader ingests from External Volume to Delta tables
3. **Silver Layer**: Data cleaning, quality validation, and enrichment
4. **Gold Layer**: dbt models create analytics-ready aggregations

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Delta Live Tables | Pipeline automation & monitoring |
| **Compute** | Databricks Serverless | Auto-scaling, zero-ops compute |
| **Storage** | Delta Lake | ACID transactions, time travel |
| **Governance** | Unity Catalog | Centralized metadata & access control |
| **Ingestion** | Auto Loader | Incremental S3 ingestion |
| **Analytics** | dbt Core | SQL-based transformations |
| **Format** | Parquet/Delta | Columnar storage |

---

## Project Structure

```
databricks/
├── resources/
│   ├── src/
│   │   ├── setup/
│   │   │   └── setup_catalog.py          # 🔧 Unity Catalog initialization
│   │   ├── bronze/
│   │   │   └── bronze_layer.py           # S3 → Bronze ingestion
│   │   ├── silver/
│   │   │   └── silver_layer.py           # Bronze → Silver transformation
│   │   └── gold/                         # (Empty - managed by dbt)
│   ├── pipelines.yml                     # DLT pipeline configuration
│   ├── daily_refresh_job.yml             # Daily pipeline orchestration
│   └── dbt_job.yml                       # dbt Gold layer job
├── schemas/
│   └── nyc_311_schema.py                 # Data schema definitions
├── databricks.yml                        # Databricks Asset Bundle config
└── README.md                             # This file
```

---

## Getting Started

### Prerequisites

Before you begin, ensure you have:

- ✅ **Databricks CLI** installed: `pip install databricks-cli`
- ✅ **Databricks workspace** with Unity Catalog enabled
- ✅ **AWS S3 access** to:
  - `s3://nyc-311-raw/` - Raw Parquet files (read-only)
  - `s3://nyc-311-data-dev/` - Managed Delta tables (write)
- ✅ **Databricks personal access token** (PAT)

### Quick Start (5 Minutes)

```bash
# 1. Configure Databricks CLI
databricks configure --token
# Enter workspace URL: https://your-workspace.cloud.databricks.com
# Enter token: dapi...

# 2. Deploy the bundle
databricks bundle deploy -t dev

# 3. ⚠️ CRITICAL: Initialize Unity Catalog (first-time only)
databricks workspace import ./resources/src/setup/setup_catalog.py \
  /Workspace/Users/$(databricks current-user me --output json | jq -r .userName)/setup_catalog.py

# 4. Run the pipeline
databricks bundle run daily_refresh_job -t dev

# 5. Verify data
databricks sql execute --warehouse-id <warehouse-id> \
  "SELECT COUNT(*) FROM nyc_311_dev.silver.silver_311"
```

### Detailed Deployment

#### Step 1: Configure Databricks CLI

```bash
databricks configure --token
```

You'll be prompted for:
- **Host**: Your Databricks workspace URL
- **Token**: Personal access token (generate in User Settings → Developer → Access Tokens)

#### Step 2: Deploy Bundle

```bash
# Deploy to development environment
databricks bundle deploy -t dev

# For production deployment
databricks bundle deploy -t prod
```

This command:
- ✅ Validates YAML configurations
- ✅ Uploads source code to workspace
- ✅ Creates Lakeflow Declarative Pipelines (DLT)  pipelines
- ✅ Configures scheduled jobs

#### Step 3: ⚠️ CRITICAL - Run Catalog Setup First

> [!WARNING]
> **You MUST initialize Unity Catalog before running the pipeline. Without this step, the pipeline will fail!**

**Option 1: Run via Databricks CLI (recommended)**
```bash
databricks workspace import ./resources/src/setup/setup_catalog.py \
  /Workspace/Users/$(databricks current-user me --output json | jq -r .userName)/setup_catalog.py

# Manually execute the notebook in Databricks UI
```

**Option 2: Run in Databricks notebook**
1. Upload `setup_catalog.py` to Databricks workspace
2. Attach to a cluster
3. Run all cells

**What this script does:**
- ✅ Creates Unity Catalog (`nyc_311_dev` or `nyc_311_prod`)
- ✅ Creates schemas: `bronze`, `silver`, `gold`
- ✅ Creates External Volume pointing to `s3://nyc-311-raw/`
- ✅ Creates checkpoint volumes for Lakeflow Declarative Pipelines (DLT)  incremental processing
- ✅ Sets up managed storage location

#### Step 4: Start Delta Live Tables Pipeline

```bash
# Run the daily refresh job (includes DLT pipeline)
databricks bundle run daily_refresh_job -t dev

# Monitor pipeline in Databricks UI
# Navigate to: Workflows → delta_live_tables_nyc_311_dev
```

**Expected output:**
```
Bronze Layer: ~1 min 53 sec (42M records ingested)
Silver Layer: ~7 min 5 sec (42M records transformed)
Total: ~9 minutes
```

#### Step 5: Run dbt Gold Layer (Optional)

```bash
# Run dbt transformations after Silver layer is populated
databricks bundle run dbt_run_daily -t dev
```

**Verify Gold layer tables:**
```sql
SHOW TABLES IN nyc_311_dev.gold;
```

---

## Data Layers

### Bronze Layer (Raw Data Ingestion)

**File**: `src/bronze/bronze_layer.py`

**Purpose**: Ingest raw Parquet files from S3 External Volume with minimal transformation

**Features**:
- **Auto Loader**: Incremental ingestion with schema evolution
- **External Volume**: Reads from `/Volumes/nyc_311_dev/bronze/raw_files/` → `s3://nyc-311-raw/`
- **Schema Evolution**: Handles new columns automatically (42 columns → future-proof)
> ⚠️ **Note**: Bronze layer schema is defined in code but not enforced. 
> Auto Loader dynamically infers schema. For strict schema enforcement, 
> add `.schema(nyc_311_schema)` to the readStream configuration.
- **Metadata Tracking**: 
  - `_ingested_at`: Ingestion timestamp
  - `_source_file`: Source file path
  - `_file_modification_time`: File last modified
  - `_file_size`: File size in bytes

**Table**: `nyc_311_dev.bronze.nyc_311_raw`

**Data Coverage**: 2010-2025 (~42M records)

---

### Silver Layer (Data Quality & Enrichment)

**File**: `src/silver/silver_layer.py`

**Purpose**: Clean, validate, enrich, and standardize Bronze data

#### Key Transformations

**1. Borough Recovery (3-Tier Hierarchy)** 🎯
- **Tier 1**: Keep original borough if valid
- **Tier 2**: Recover from ZIP code mapping (100K+ records)
- **Tier 3**: Recover from neighborhood/city mapping (75K+ records)
- **Result**: 98.6% → **99.0% borough coverage**

**2. City Granularity Preservation** 🏙️
- Maintains ~2,498 distinct city values (neighborhoods)
- Borough mapping only applied when city is NULL
- Examples: "WILLIAMSBURG", "ASTORIA", "PARK SLOPE" preserved

**3. Closed Date Inference** 📅
- **DHS agency records**: Use `resolution_action_updated_date` when `closed_date` is NULL
- **Coverage**: 53,679 records fixed (99.88% accuracy)
- **Validation**: Only for CLOSED status, temporal consistency checks
- **Audit trail**: `closed_date_source` column tracks inference

**4. Data Quality Flags** ✅
```python
has_valid_location          # Valid lat/long coordinates (NYC bounds)
has_valid_zip               # 5-digit ZIP validation
has_resolution              # Resolution description exists
has_invalid_closed_date     # closed_date < created_date (data error)
has_future_closed_date      # closed_date > 30 days from now
has_closed_status_without_date  # CLOSED status but no closed_date
open_status_with_closed_date    # Open status but has closed_date
has_historical_closed_date # closed_date before 2010 (dataset start year)
```

**5. Geographic Validation** 🗺️
- NYC boundary validation (40.477-40.918 lat, -74.259 to -73.700 lon)
- ZIP code format validation (5-digit pattern)
- Coordinate nullification for out-of-bounds values

**6. String Standardization** 📝
- UPPERCASE normalization
- Trimming whitespace
- Special handling for resolution_description by status
- Address type classification (INTERSECTION, BLOCK, ADDRESS)

#### Data Quality Metrics

**Expected Results:**

✅ **Borough Coverage**:
```
Before: UNSPECIFIED: 565,667 records
After:  UNSPECIFIED: ~390,000 records (31% reduction)
- ZIP-based recovery: ~100,000 records
- Neighborhood recovery: ~75,000 records
```

✅ **Closed Date Inference**:
```
DHS records fixed: 53,679
Accuracy: 99.88%
Source tracking: closed_date_source column
```

✅ **Data Quality**:
```
Valid locations: 85%+
Valid ZIP codes: 93%+
Borough coverage: 99.0%
```

**Table**: `nyc_311_dev.silver.silver_311`

**Update Strategy**: SCD Type 1 (MERGE on `unique_key`, latest by `updated_date`)

**Partitioning**: By `created_year`, `created_month` for query optimization

---

### Gold Layer (Analytics - dbt)

**Location**: Managed by dbt in `/dbt_nyc_311/` (separate project)

**Purpose**: Create analytics-ready aggregations and dimensional models

---

## Configuration

### Unity Catalog Structure

```
nyc_311_dev/                          # Catalog (dev environment)
├── bronze/                           # Schema
│   ├── nyc_311_raw                   # Delta table (managed)
│   ├── raw_files/                    # External Volume → s3://nyc-311-raw/
│   └── checkpoints/                  # Auto Loader checkpoints
├── silver/                           # Schema
│   ├── silver_311                    # Delta table (managed)
│   └── checkpoints/                  # DLT checkpoints
└── gold/                             # Schema
    └── (dbt-managed tables)
```

**View catalog structure:**
```sql
-- List all schemas
SHOW SCHEMAS IN nyc_311_dev;

-- List tables in Silver schema
SHOW TABLES IN nyc_311_dev.silver;

-- Describe table details
DESCRIBE EXTENDED nyc_311_dev.silver.silver_311;
```

### Environment Variables

| Environment | Catalog | Data Bucket | Target |
|-------------|---------|-------------|--------|
| **Development** | `nyc_311_dev` | `s3://nyc-311-data-dev/` | `-t dev` |
| **Production** | `nyc_311_prod` | `s3://nyc-311-data-prod/` | `-t prod` |

**Switch environments:**
```bash
# Deploy to development
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```
### Pipeline Configuration

**File**: `resources/pipelines.yml`

Defines the Delta Live Tables pipeline with serverless compute, Bronze/Silver layer processing, and Unity Catalog integration.

### Job Schedules

**Daily Refresh Job** (`daily_refresh_job.yml`):

Triggers DLT pipeline daily at 7:00 AM EST to ingest and transform new 311 data (Bronze → Silver layers).

**dbt Job** (`dbt_job.yml`):

Runs dbt transformations daily at 9:00 AM EST to refresh Gold layer analytics tables after Silver layer completion.

---

## Observability & Monitoring

### Delta Live Tables UI

Navigate to **Workflows → Delta Live Tables** in Databricks UI:

- 📊 **Pipeline Runs**: View execution history and duration
- 🔄 **Data Lineage**: Visual graph of Bronze → Silver → Gold flow
- ✅ **Data Quality Metrics**: Track expectation pass/fail rates
- 📉 **Performance Trends**: Monitor processing times over time

### Table History & Time Travel

```sql
-- View table history (last 30 days)
DESCRIBE HISTORY nyc_311_dev.silver.silver_311;

-- Query data as of specific version
SELECT * 
FROM nyc_311_dev.silver.silver_311 
VERSION AS OF 10;

-- Restore to previous version
RESTORE TABLE nyc_311_dev.silver.silver_311 TO VERSION AS OF 10;
```

### Delta Metrics

```sql
-- Table statistics
DESCRIBE DETAIL nyc_311_dev.silver.silver_311;
```

## Performance

### Performance Benchmarks

**Actual Processing Times** (serverless, production run):

| Layer | Duration | Records | Throughput | Details |
|-------|----------|---------|------------|---------|
| **Bronze** | 1 min 53 sec | 42M | ~360K rec/sec | S3 → Delta (Auto Loader) |
| **Silver** | 7 min 5 sec | 42M | ~99K rec/sec | Enrichment + MERGE |
| **Total** | **~9 minutes** | 42M | ~78K rec/sec | End-to-end pipeline |

**Per Million Records** (estimated):
- Bronze: ~2.7 seconds per 1M records
- Silver: ~10 seconds per 1M records

**Storage Efficiency** (production verified):

| Layer | Size | Records | Files | Compression | Avg/Record |
|-------|------|---------|-------|-------------|------------|
| **Bronze** | 3.88 GB | 42M | 42 | Parquet | 92 bytes |
| **Silver** | 3.61 GB | 42M | 191 | Delta | 86 bytes |

**Key Insights:**
- ✅ Silver layer is **7% more space-efficient** despite enrichment (quality flags, recovery columns)
- ✅ Auto-compaction reduces file count over time (191 files → target: ~30 files @ 128MB each)
- ✅ Partitioning enables **10x faster queries** with partition pruning


#### Partitioning Strategy

| Layer | Partitioning | Rationale |
|-------|--------------|-----------|
| **Bronze** | None | Append-only, optimized for write throughput |
| **Silver** | `created_year`, `created_month` | Enables partition pruning for date queries |


## Troubleshooting

### Issue: DLT Pipeline Fails on First Run

**Symptoms:**
```
Error: Schema 'nyc_311_dev.bronze' does not exist
Error: Volume 'raw_files' not found
```

**Cause**: Unity Catalog not initialized

**Solution**: 
```bash
# Run setup_catalog.py first (see Step 3 in deployment)
databricks workspace import ./resources/src/setup/setup_catalog.py \
  /Workspace/Users/<your-email>/setup_catalog.py

# Execute the notebook in Databricks UI
```

---

### Issue: Schema Evolution Error

**Symptoms:**
```
Error: Column 'new_column_name' does not exist in target table
```

**Cause**: New columns in source data, but schema merging disabled

**Solution**: Auto Loader handles this automatically with:
```python
# In bronze_layer.py (already configured)
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

If still failing:
```sql
-- Manually add column to Bronze table
ALTER TABLE nyc_311_dev.bronze.nyc_311_raw 
ADD COLUMNS (new_column_name STRING);
```

---

## Development Workflow

### Making Changes

1. **Modify source files** in `resources/src/`
2. **Test locally** (optional):
   ```bash
   # Use Databricks Connect (if configured)
   python resources/src/silver/silver_layer.py
   ```
3. **Deploy changes**:
   ```bash
   databricks bundle deploy -t dev
   databricks bundle run daily_refresh_job -t dev
   ```
4. **Verify results**:
   ```sql
   SELECT COUNT(*) FROM nyc_311_dev.silver.silver_311;
   ```

## Data Schema

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `has_resolution` | BOOLEAN | Has resolution description | `resolution_description IS NOT NULL` |
| `is_closed` | BOOLEAN | Request is closed | `status = 'Closed'` |
| `has_invalid_closed_date` | BOOLEAN | Closed before created | `closed_date < created_date` |
| `has_future_closed_date` | BOOLEAN | Closed date in far future | `closed_date > CURRENT_DATE + 30 days` |
| `has_closed_status_without_date` | BOOLEAN | Closed but no date | `status = 'Closed' AND closed_date IS NULL` |
| `open_status_with_closed_date` | BOOLEAN | Open but has closed date | `status != 'Closed' AND closed_date IS NOT NULL` |

#### Processing Metadata (1 column)
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `_unified_processed_timestamp` | TIMESTAMP | When Silver processing occurred | "2024-01-21 08:15:00" |

---

## Security & Governance

### Unity Catalog Benefits

| Feature | Benefit | Implementation |
|---------|---------|----------------|
| **Centralized Governance** | Single source of truth for data assets | All tables in `nyc_311_dev` catalog |
| **Access Control** | Fine-grained permissions | Catalog/Schema/Table level grants |
| **Data Lineage** | Track data flow Bronze→Silver→Gold | DLT automatically tracks lineage |
| **Audit Logging** | All access/modifications logged | Unity Catalog audit logs |
| **Data Discovery** | Search and documentation | Table comments, tags, descriptions |

## FAQ

<details>
<summary><b>How do I handle schema changes from the source?</b></summary>

Auto Loader automatically handles new columns with `schemaEvolutionMode: addNewColumns`. Existing columns cannot be dropped or modified without manual intervention.

**For new columns:**
- Bronze layer automatically adapts
- Update Silver layer transformation logic if needed
- Redeploy bundle: `databricks bundle deploy -t dev`

**For column deletions/renames:**
```sql
-- Manually modify schema
ALTER TABLE nyc_311_dev.bronze.nyc_311_raw DROP COLUMN old_column;
ALTER TABLE nyc_311_dev.bronze.nyc_311_raw RENAME COLUMN old_name TO new_name;
```
</details>

<details>
<summary><b>What's the cost of running this pipeline?</b></summary>

**Serverless Compute** Databricks Free Edition

</details>

<details>
<summary><b>Can I use this with Databricks SQL Warehouses?</b></summary>

Yes! After the pipeline runs, query tables using Databricks SQL:

Create dashboards in Databricks SQL UI for business users.
</details>

---

## Resources

### Databricks Documentation
- [Lakeflow Declarative Pipelines](https://docs.databricks.com/delta-live-tables/) - Pipeline orchestration
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/) - Data governance
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/) - Incremental ingestion
- [Delta Lake Guide](https://docs.databricks.com/delta/index.html) - ACID transactions
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) - IaC deployment

### Delta Lake Resources
- [Delta Lake Optimization](https://docs.delta.io/latest/optimizations-oss.html) - Performance tuning
- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html) - Production tips
- [Time Travel](https://docs.delta.io/latest/delta-batch.html#deltatimetravel) - Historical queries

### NYC Open Data
- [NYC 311 Dataset](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) - Source data

### Community & Support
- [Databricks Community](https://community.databricks.com/) - Forums and Q&A

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Contribution Guidelines

- Follow PySpark coding standards
- Update documentation (README + inline comments)
- Test in dev environment before submitting PR
- Include performance benchmarks for major changes

---

## 📊 Project Stats

![Lines of Code](https://img.shields.io/badge/Lines_of_Code-2.5K-blue)
![Data Processed](https://img.shields.io/badge/Data_Processed-42M_records-green)
![Pipeline Runtime](https://img.shields.io/badge/Pipeline_Runtime-9_minutes-orange)
![Data Quality](https://img.shields.io/badge/Data_Quality-99%25-brightgreen)

**Last Updated**: November 2025  
**Version**: 1.0.0  
**Databricks Runtime**: 14.3 LTS  
**Delta Lake**: 3.0+  
**Python**: 3.10+

---

<div align="center">

**Built with ❤️ using Databricks**

[⬆ Back to Top](#nyc-311-data-lakehouse-pipeline)

</div>