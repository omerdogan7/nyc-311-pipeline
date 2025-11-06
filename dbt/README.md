![Tech Stack Animation](/assets/dbt-logo-w.png)

# 🗽 NYC 311 Gold Layer - dbt Project

**Enterprise Data Warehouse for NYC 311 Service Request Analytics**

[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00ADD4?style=for-the-badge)](https://docs.databricks.com/data-governance/unity-catalog/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)


---

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Data Models](#data-models)
- [Performance](#performance)
- [Testing](#testing)
- [Maintenance](#maintenance)

---

##  Overview

This dbt project transforms NYC 311 service request data from the **Silver layer** into **Gold layer** analytics-ready models. The project implements:

- ⭐ **Star Schema** with fact and dimension tables
- 📊 **Pre-aggregated** summary tables for performance
- 🧠 **ML-ready** feature engineering
- 🔍 **Advanced analytics** (COVID impact analysis)
- 🚀 **Optimized** with partitioning & clustering

### Key Features
- **Incremental loading** with watermark-based processing
- **Delta Lake** optimization with Z-ordering
- **Comprehensive testing** (data quality, referential integrity)
- **Detailed documentation** with business context
- **Production-ready** for enterprise analytics

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                             │
│                   (silver_311 table)                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (dbt)                         │
│                                                               │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Dimensions │  │     Facts     │  │ Aggregations │       │
│  │             │  │               │  │              │       │
│  │ • Agency    │  │ • Complaints  │  │ • Daily      │       │
│  │ • Location  │  │   (50+ cols)  │  │ • Monthly    │       │
│  │ • Complaint │  │               │  │ • Agency     │       │
│  │ • Date      │  │               │  │ • Location   │       │
│  └─────────────┘  └──────────────┘  │ • Complaint  │       │
│                                      └──────────────┘       │
│                                                               │
│  ┌─────────────┐  ┌──────────────┐                          │
│  │  Analytics  │  │ ML Features  │                          │
│  │             │  │              │                          │
│  │ • COVID     │  │ • Prophet    │                          │
│  │   Impact    │  │   Features   │                          │
│  │             │  │ • Time Series│                          │
│  └─────────────┘  └──────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
nyc_311_gold/
├── models/
│   ├── marts/
│   │   ├── core/                    # Star schema (facts & dimensions)
│   │   │   ├── dimensions/
│   │   │   │   ├── dim_agency.sql
│   │   │   │   ├── dim_location.sql
│   │   │   │   ├── dim_complaint_type.sql
│   │   │   │   ├── dim_date.sql
│   │   │   │   └── _schema.yml
│   │   │   ├── facts/
│   │   │   │   ├── fact_311.sql             # Main fact table, incremental
│   │   │   │   └── _schema.yml
│   │   │   └── aggregations/
│   │   │       ├── agg_daily_summary.sql
│   │   │       ├── agg_monthly_summary.sql
│   │   │       ├── agg_agency_metrics.sql
│   │   │       ├── agg_location_metrics.sql
│   │   │       └── agg_complaint_type_metrics.sql
│   │   ├── analytics/               # Business insights
│   │   │   ├── covid_impact_analysis.sql
│   │   │   └── _schema.yml
│   │   └── ml_features/             # ML feature engineering
│   │       ├── volume_forecasting_features.sql
│   │       └── _schema.yml
│   └── sources.yml                  # Silver layer source definition
├── macros/
│   └── generate_schema_name.sql     # Custom schema logic
├── tests/
│   ├── assert_closed_after_created.sql
│   ├── assert_response_time_consistency.sql
│   └── custom_tests.yml
├── dbt_project.yml                  # Project configuration
├── packages.yml                     # dbt_utils dependency
└── README.md                        # This file
```

---

## Getting Started

### Prerequisites
- dbt >= 1.5.0
- Databricks workspace with Delta Lake
- Python 3.8+
- Access to Silver layer (`silver_311` table)

### Installation

1. **Clone the repository**
```bash
git clone <repo-url>
cd nyc_311_project/dbt/nyc_311_gold
```

2. **Install dbt packages**
```bash
dbt deps
```

3. **Configure profile** (`~/.dbt/profiles.yml`)
```yaml
nyc_311_gold:
  outputs:
    dev:
      type: databricks
      host: <your-databricks-host>
      http_path: <your-cluster-http-path>
      schema: gold
      catalog: nyc_311_dev
      token: <your-token>
    prod:
      type: databricks
      host: <your-databricks-host>
      http_path: <your-cluster-http-path>
      schema: gold
      catalog: nyc_311_prod
      token: <your-token>
  target: dev
```

4. **Test connection**
```bash
dbt debug
```

### Running the Project

```bash
# Full refresh (first time)
dbt run --full-refresh

# Incremental run (daily)
dbt run

# Run specific model
dbt run --select fact_311

# Run with tests
dbt build

# Run only facts
dbt run --select tag:fact

# Run only incremental models
dbt run --select tag:incremental
```

---

## Data Models

### Core Models (Star Schema)
![Tech Stack Animation](/assets/star_schema.gif)
#### **Dimensions**

| Model | Description | Rows (Est.) | Key | Refresh |
|-------|-------------|-------------|-----|---------|
| `dim_agency` | NYC agencies with categories | ~50 | agency_key | Full |
| `dim_location` | Borough/city/zip hierarchy | ~5,000 | location_key | Full |
| `dim_complaint_type` | Complaint types with descriptors | ~400 | complaint_type_key | Full |
| `dim_date` | Date dimension (2010-2030) | 7,670 | date_key | Static |

#### **Facts**

| Model | Description | Rows (Est.) | Grain | Strategy |
|-------|-------------|-------------|-------|----------|
| `fact_311` | All 311 complaints | ~42M+ | One row per complaint | Incremental (merge) |

**Fact Columns** (50+ total):
- **Keys**: unique_key, agency_key, location_key, complaint_type_key, created_date_key, closed_date_key
- **Denormalized Attributes**: borough, city, agency_code, complaint_type, descriptor
- **Time Attributes**: created_year, created_month, created_hour, is_business_hours
- **Date Flags**: is_created_on_weekend, is_created_on_holiday, is_created_on_business_day, created_fiscal_year
- **Measures**: response_time_hours, response_time_days
- **Categories**: status, response_sla_category
- **Status Flags**: is_closed, has_resolution
- **Location Details**: latitude, longitude, location_type, has_valid_location, incident_zip, incident_address, community_board, bbl
- **Data Quality Flags**: has_invalid_closed_date, has_historical_closed_date, has_future_closed_date, has_valid_zip
- **Metadata**: closed_date_source, closed_date_original, open_data_channel_type, _source_file, _ingested_at, _unified_processed_timestamp, loaded_at

#### **Aggregations**

| Model | Description | Grain | Update |
|-------|-------------|-------|--------|
| `agg_daily_summary` | Daily KPIs & trends | Per day | Incremental |
| `agg_monthly_summary` | Monthly trends with YoY | Per month | Incremental |
| `agg_agency_metrics` | Agency performance metrics | Per agency per day | Incremental |
| `agg_location_metrics` | Location hotspot analysis | Per location per day | Incremental |
| `agg_complaint_type_metrics` | Complaint type analysis | Per complaint type per day | Incremental |

### Analytics Models

| Model | Purpose | Use Case |
|-------|---------|----------|
| `covid_impact_analysis` | Pre/peak/post COVID analysis | Research, executive reports |

### ML Features

| Model | Purpose | Features |
|-------|---------|----------|
| `volume_forecasting_features` | Time-series forecasting | Temporal features for Prophet |

**Prophet-ready format**: `ds` (date), `y` (target), plus regressors

---

## Performance

#### Full Refresh (Initial Load)
```bash
dbt run --full-refresh 
# Duration: 3 min 34 sec | Models: 12
```

## Testing

### Test Coverage

- **Schema tests**: 86 (unique, not_null, relationships, accepted_values)
- **Custom tests**: Data logic validation

### Running Tests
```bash
# All tests
dbt test

# Specific model
dbt test --select fact_311

# Severity levels
dbt test --severity warn  # Only warnings
dbt test --severity error # Only errors
```

### Test Categories

**Data Quality**
- Non-null constraints
- Unique keys
- Accepted value ranges
- Date logic (closed >= created)

**Referential Integrity**
- Foreign key relationships
- Dimension existence

**Business Rules**
- Response time >= 0
- Status transitions logical
- Data quality flags

---

## Maintenance

### Daily Tasks
```bash
# Incremental load
dbt run --select tag:incremental

# Test new data
dbt test --select tag:incremental

# Check freshness
dbt source freshness
```

### Weekly Tasks
```bash
# Full dimension refresh
dbt run --select tag:dimension --full-refresh

# Optimize tables
OPTIMIZE gold.fact_311 
WHERE created_year >= year(date_sub(current_date(), 90)) 
ZORDER BY (agency_code, created_month)

# Review logs
cat logs/dbt.log | grep ERROR
```

### Monthly Tasks
```bash
# Vacuum old files
VACUUM gold.fact_311 RETAIN 168 HOURS

# Update statistics
ANALYZE TABLE gold.fact_311 COMPUTE STATISTICS FOR ALL COLUMNS

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## 📊 Model Dependencies (DAG)

```
silver_311
    │
    ├─→ dim_agency
    ├─→ dim_location
    ├─→ dim_complaint_type
    ├─→ dim_date
    │
    └─→ fact_311
         │
         ├─→ agg_daily_summary
         ├─→ agg_monthly_summary
         ├─→ agg_agency_metrics
         ├─→ agg_location_metrics
         ├─→ agg_complaint_type_metrics
         │
         ├─→ covid_impact_analysis
         │
         └─→ volume_forecasting_features
```

---

##  Documentation

### Generated Docs
```bash
# Generate documentation
dbt docs generate

# Serve locally
dbt docs serve --port 8080
```

Visit: http://localhost:8080

### Additional Information
- Model descriptions in `_schema.yml` files
- Source definitions in `sources.yml`
- Project configuration in `dbt_project.yml`

---

## 🐛 Troubleshooting

### Common Issues

**Issue**: Incremental model not updating
```bash
# Check watermark
SELECT MAX(_unified_processed_timestamp) FROM gold.fact_311;

# Force refresh
dbt run --select fact_311 --full-refresh
```

**Issue**: Slow queries
```bash
# Check if optimized
DESCRIBE DETAIL gold.fact_311;

# Run optimization
OPTIMIZE gold.fact_311 
WHERE created_year >= year(date_sub(current_date(), 90)) 
ZORDER BY (agency_code, created_month)
```

**Issue**: Test failures
```bash
# See failures
dbt test --store-failures

# Debug specific test
dbt test --select fact_311 --store-failures
```

---

## 🤝 Contributing

### Adding New Models

1. Create model in appropriate folder
2. Add schema documentation in `_schema.yml`
3. Add tests (unique, not_null, relationships)
4. Update this README
5. Run `dbt run` and `dbt test`
6. Submit PR


## 📄 License

MIT License 

---

**Version**: 1.0  
**Last Updated**: November 2025  


