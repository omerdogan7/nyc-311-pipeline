# NYC 311 Service Requests Data Pipeline

## Project Overview

This project implements a comprehensive, production-grade data engineering pipeline for NYC 311 service request data. The pipeline follows the Medallion Architecture (Bronze-Silver-Gold) and leverages modern data engineering tools including Apache Airflow, Databricks, dbt, and AWS S3.

## Architecture

### High-Level Data Flow

```
NYC 311 API → Airflow → AWS S3 (Raw) → Databricks (Bronze) → Databricks (Silver) → dbt (Gold) → Analytics/ML
```

### Technology Stack

- **Orchestration**: Apache Airflow (Astronomer)
- **Storage**: AWS S3
- **Processing**: Databricks (PySpark)
- **Transformation**: dbt (Data Build Tool)
- **Infrastructure**: Terraform
- **Testing**: pytest
- **Version Control**: Git

## Data Pipeline Layers

### 1. Ingestion Layer (Airflow)

**Location**: `ingestion/`

The ingestion layer uses Apache Airflow to fetch data from the NYC 311 API and land it in AWS S3.

#### Components:

- **`nyc311_daily_ingestion.py`**: DAG for daily incremental data ingestion
  - Runs daily to fetch new service requests
  - Uses SoQL queries to fetch data efficiently
  - Implements retry logic and error handling
  - Lands data in S3 as Parquet files

- **`nyc311_historical_backfill.py`**: DAG for historical data backfill
  - Processes historical data in batches
  - Handles large-scale data ingestion
  - Implements date-based partitioning

#### Key Features:

- **Incremental Loading**: Only fetches new/updated records
- **Data Partitioning**: Organizes data by date for efficient processing
- **Error Handling**: Comprehensive retry logic and alerting
- **Data Quality**: Basic validation before landing to S3

#### Configuration:

```python
# Environment variables required:
NYC_311_APP_TOKEN=<your_app_token>
AWS_ACCESS_KEY_ID=<your_aws_key>
AWS_SECRET_ACCESS_KEY=<your_aws_secret>
S3_BUCKET_NAME=<your_bucket_name>
```

### 2. Bronze Layer (Databricks - Raw Data)

**Location**: `databricks/resources/src/bronze/`

The Bronze layer ingests raw data from S3 into Databricks Delta tables with minimal transformation.

#### Purpose:

- Store raw data exactly as received from the source
- Maintain data lineage and audit trail
- Enable data replay and reprocessing if needed
- Append-only storage for historical tracking

#### Implementation:

**File**: `bronze_layer.py`

- Reads Parquet files from S3
- Adds metadata columns:
  - `_ingested_at`: Timestamp when data was loaded
  - `_source_file`: Source file path for tracking
- Writes to Delta table with append mode
- Maintains schema evolution capabilities

#### Key Features:

- **Schema on Read**: Flexible schema handling
- **Data Versioning**: Delta Lake time travel capabilities
- **Audit Trail**: Complete data lineage tracking
- **Idempotency**: Safe to rerun without duplicates (when combined with deduplication in Silver)

### 3. Silver Layer (Databricks - Cleaned Data)

**Location**: `databricks/resources/src/silver/`

The Silver layer transforms Bronze data through multiple stages of cleaning and enrichment.

#### Pipeline Stages:

1. **`01_bronze_to_silver_base.py`**: Initial transformation
   - Schema standardization
   - Data type conversions
   - Basic null handling
   - Column renaming to standard conventions

2. **`02_silver_data_quality.py`**: Data quality checks
   - Null value validation
   - Data type validation
   - Business rule validation
   - Quality flags addition:
     - `has_valid_location`: Validates coordinates
     - `has_valid_zip`: Validates ZIP codes
     - `has_resolution`: Checks for resolution description
     - `has_invalid_closed_date`: Identifies data quality issues
     - `has_future_closed_date`: Flags suspicious dates

3. **`03_silver_enrichments.py`**: Data enrichment
   - Date dimension extraction (year, month, day)
   - Geographic enrichment
   - Status standardization
   - Address normalization

4. **`04_silver_final.py`**: Final silver table creation
   - Deduplication based on `unique_key`
   - Merge operation for upserts
   - Final quality checks
   - Optimized table structure

#### Key Features:

- **Data Quality Framework**: Comprehensive validation rules
- **Deduplication**: Ensures unique records
- **Change Data Capture**: Tracks updates via merge operations
- **Partitioning**: Optimized by `created_date` for query performance
- **Z-Ordering**: Optimized for common query patterns

### 4. Gold Layer (dbt - Business-Ready Data)

**Location**: `dbt_nyc_311/`

The Gold layer creates business-ready analytical models using dbt.

#### Model Structure:

**Dimensions** (`models/marts/core/dimensions/`):
- `dim_date.sql`: Date dimension with calendar attributes
- `dim_agency.sql`: Agency lookup with descriptions
- `dim_complaint_type.sql`: Complaint type categorization
- `dim_location.sql`: Geographic dimension (borough, ZIP, coordinates)

**Facts** (`models/marts/core/facts/`):
- `fact_311.sql`: Main fact table with all service requests
  - Links to all dimension tables via surrogate keys
  - Calculates response times
  - Includes all quality flags from Silver layer

**Aggregations** (`models/marts/core/aggregations/`):
- `agg_daily_summary.sql`: Daily complaint metrics
- `agg_monthly_summary.sql`: Monthly trend analysis
- `agg_agency_metrics.sql`: Performance by agency
- `agg_complaint_type_metrics.sql`: Volume by complaint type
- `agg_location_metrics.sql`: Geographic analysis

**Analytics Models** (`models/marts/analytics/`):
- `seasonal_trends.sql`: Seasonal pattern analysis
- `covid_impact_analysis.sql`: Pre/during/post-COVID comparisons
- `holiday_pattern_analysis.sql`: Holiday impact on complaint volumes

**ML Features** (`models/marts/ml_features/`):
- `volume_forecasting_features.sql`: Time series features for forecasting
  - Day of week, month, quarter features
  - Holiday indicators
  - Rolling averages and trends
  - Lag features

#### dbt Features:

- **Testing**: Comprehensive data quality tests
  - Uniqueness constraints
  - Referential integrity checks
  - Not null validations
  - Accepted value ranges
- **Documentation**: Inline documentation with descriptions
- **Incremental Models**: Efficient processing of large datasets
- **Macros**: Custom macros for Delta Lake optimization

## Project Structure

```
nyc_311_project/
├── ingestion/                    # Airflow orchestration
│   ├── dags/
│   │   ├── nyc311_daily_ingestion.py
│   │   └── nyc311_historical_backfill.py
│   ├── include/
│   │   └── nyc311_ingestion.py   # Core ingestion logic
│   └── tests/
│
├── databricks/                   # Databricks processing
│   ├── resources/
│   │   ├── src/
│   │   │   ├── bronze/          # Raw data ingestion
│   │   │   │   └── bronze_layer.py
│   │   │   ├── silver/          # Data cleaning & enrichment
│   │   │   │   ├── 01_bronze_to_silver_base.py
│   │   │   │   ├── 02_silver_data_quality.py
│   │   │   │   ├── 03_silver_enrichments.py
│   │   │   │   └── 04_silver_final.py
│   │   │   ├── setup/           # Initial setup
│   │   │   │   └── setup_catalog.py
│   │   │   └── jobs/            # Monitoring jobs
│   │   │       └── daily_monitoring.py
│   │   ├── pipelines.yml        # DLT pipeline configuration
│   │   ├── setup_job.yml        # Setup job configuration
│   │   └── monitoring_job.yml   # Monitoring job configuration
│   ├── tests/
│   │   ├── unit/               # Unit tests
│   │   └── integration/        # Integration tests
│   └── databricks.yml          # Databricks bundle config
│
├── dbt_nyc_311/                 # dbt transformations
│   ├── models/
│   │   ├── marts/
│   │   │   ├── core/
│   │   │   │   ├── dimensions/
│   │   │   │   ├── facts/
│   │   │   │   └── aggregations/
│   │   │   ├── analytics/
│   │   │   └── ml_features/
│   │   └── sources.yml
│   ├── macros/
│   ├── tests/
│   └── dbt_project.yml
│
├── terraform/                   # Infrastructure as code
│   ├── main.tf
│   ├── bucket.tf               # S3 bucket configuration
│   ├── variables.tf
│   └── outputs.tf
│
└── README.md                   # This file
```

## Setup and Installation

### Prerequisites

- Python 3.11+
- AWS Account with S3 access
- Databricks Workspace
- NYC Open Data API Token
- Docker (for Airflow)
- Terraform (for infrastructure)

### 1. Infrastructure Setup (Terraform)

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

This creates:
- S3 bucket for raw data storage
- IAM roles and policies

### 2. Airflow Setup

```bash
cd ingestion/

# Install Astronomer CLI
curl -sSL install.astronomer.io | sudo bash -s

# Initialize Airflow
astro dev init

# Configure environment variables
cp .env.example .env
# Edit .env with your credentials

# Start Airflow
astro dev start
```

Access Airflow UI at `http://localhost:8080`

### 3. Databricks Setup

```bash
cd databricks/

# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Deploy bundle
databricks bundle deploy -t dev

# Run setup job
databricks bundle run setup_job -t dev
```

### 4. dbt Setup

```bash
cd dbt_nyc_311/

# Install dbt
pip install dbt-databricks

# Configure profiles.yml with Databricks connection

# Install dependencies
dbt deps

# Run models
dbt run

# Run tests
dbt test
```

## Data Flow Details

### Ingestion (Airflow → S3)

1. **API Fetch**: Airflow calls NYC 311 API using SoQL queries
2. **Pagination**: Handles large datasets with pagination
3. **Transformation**: Minimal transformation, mostly data type casting
4. **Storage**: Writes to S3 as Parquet files
   - Path: `s3://bucket-name/raw/nyc-311/year=YYYY/month=MM/day=DD/`
   - Partitioned by date for efficient access

### Bronze Layer (S3 → Databricks)

1. **Auto Loader**: Uses Databricks Auto Loader for incremental ingestion
2. **Schema Inference**: Automatically infers schema from Parquet files
3. **Metadata Addition**: Adds audit columns
4. **Delta Write**: Writes to Delta table
   - Location: `catalog.bronze.nyc_311_bronze`
   - Mode: Append-only

### Silver Layer (Databricks DLT)

Runs as Delta Live Tables pipeline with 4 stages:

1. **Base Transformation**: Clean and standardize data
2. **Quality Checks**: Add data quality flags
3. **Enrichment**: Add derived columns and dimensions
4. **Final Merge**: Deduplicate and create final Silver table
   - Location: `catalog.silver.nyc_311_silver`
   - Mode: Merge (upsert on unique_key)

### Gold Layer (dbt)

Runs nightly as dbt job:

1. **Dimensions**: Build slowly changing dimensions
2. **Facts**: Create fact table with surrogate keys
3. **Aggregations**: Pre-calculate common metrics
4. **Analytics**: Build specialized analytics tables
5. **ML Features**: Generate features for ML models

## Monitoring and Observability

### Data Quality Metrics

**Bronze Layer:**
- Record count by ingestion date
- File count and size metrics
- Schema drift detection

**Silver Layer:**
- Data quality flag percentages
- Null value tracking
- Duplicate detection
- Date range validation

**Gold Layer:**
- Row count validations
- Relationship integrity checks
- Business rule violations
- Test failures in dbt

### Monitoring Dashboard

Location: `databricks/resources/src/jobs/daily_monitoring.py`

Tracks:
- Pipeline execution times
- Data freshness
- Quality metrics over time
- Error rates
- Volume trends

## Testing Strategy

### Unit Tests

**Databricks** (`databricks/tests/unit/`):
- Transformation logic tests
- Data quality rule tests
- Schema validation tests

**dbt** (`dbt_nyc_311/tests/`):
- Custom data tests
- Relationship tests
- Business logic tests

### Integration Tests

**Databricks** (`databricks/tests/integration/`):
- End-to-end pipeline tests
- Cross-layer validation
- Performance tests

**Airflow** (`ingestion/tests/`):
- DAG validation tests
- Task dependency tests
- Schedule tests

### Running Tests

```bash
# Databricks tests
cd databricks/
pytest tests/

# dbt tests
cd dbt_nyc_311/
dbt test

# Airflow tests
cd ingestion/
pytest tests/
```

## Data Model

### Key Tables

**Bronze:**
- `bronze.nyc_311_bronze`: Raw data from S3

**Silver:**
- `silver.nyc_311_silver`: Cleaned, validated, enriched data

**Gold Dimensions:**
- `gold.dim_date`: Date dimension
- `gold.dim_agency`: Agency lookup
- `gold.dim_complaint_type`: Complaint type taxonomy
- `gold.dim_location`: Geographic hierarchy

**Gold Facts:**
- `gold.fact_311`: Main fact table

**Gold Aggregations:**
- `gold.agg_daily_summary`
- `gold.agg_monthly_summary`
- `gold.agg_agency_metrics`
- `gold.agg_complaint_type_metrics`
- `gold.agg_location_metrics`

**Gold Analytics:**
- `gold.seasonal_trends`
- `gold.covid_impact_analysis`
- `gold.holiday_pattern_analysis`

**ML Features:**
- `gold.volume_forecasting_features`

## Performance Optimization

### Databricks

- **Partitioning**: Tables partitioned by `created_date`
- **Z-Ordering**: Optimized on frequently filtered columns
- **Caching**: Strategic use of Delta cache
- **Auto Optimize**: Enabled for production tables
- **Vacuum**: Regular cleanup of old versions

### dbt

- **Incremental Models**: Use incremental strategy for large tables
- **Model Materialization**:
  - Dimensions: Table
  - Facts: Incremental
  - Aggregations: Table
  - Analytics: View (for small datasets)
- **Clustering**: Applied on Gold tables

### S3

- **Parquet Format**: Columnar storage for efficient reads
- **Partitioning**: Date-based partitioning
- **Lifecycle Policies**: Archive old data to Glacier

## Cost Optimization

- **Spot Instances**: Use for non-critical Databricks clusters
- **Auto-scaling**: Configure cluster auto-scaling
- **Job Clusters**: Use job clusters instead of all-purpose clusters
- **S3 Intelligent Tiering**: Automatic cost optimization
- **Data Retention**: Archive data older than 7 years

## Disaster Recovery

### Backup Strategy

- **S3**: Versioning enabled for raw data
- **Delta Lake**: Time travel for 30 days
- **Incremental Backups**: Daily incremental backups
- **Cross-region Replication**: Optional for critical data

### Recovery Procedures

1. **Data Loss in Bronze**: Re-run ingestion from S3
2. **Data Loss in Silver**: Replay from Bronze layer
3. **Data Loss in Gold**: Re-run dbt models
4. **S3 Data Loss**: Restore from versioning or backups

## Security

### Access Control

- **S3**: IAM roles and bucket policies
- **Databricks**: Unity Catalog with RBAC
- **dbt**: Service principal authentication
- **Airflow**: RBAC and secret management

### Data Encryption

- **At Rest**: S3 SSE, Delta Lake encryption
- **In Transit**: TLS/SSL for all connections
- **Secrets**: AWS Secrets Manager / Databricks Secrets

## CI/CD Pipeline

### GitHub Actions Workflows

```yaml
# .github/workflows/databricks-ci.yml
- Runs on PR and main branch
- Validates bundle configuration
- Runs unit tests
- Deploys to dev environment
```

### Deployment Process

1. **Development**: Work in feature branches
2. **Testing**: Run tests locally and in CI
3. **Pull Request**: Create PR with tests passing
4. **Review**: Code review and approval
5. **Merge**: Merge to main branch
6. **Deploy**: Automatic deployment to dev
7. **Validation**: Run integration tests
8. **Production**: Manual promotion to prod

## Troubleshooting

### Common Issues

**1. Airflow DAG not running:**
- Check Airflow scheduler is running
- Verify environment variables are set
- Check DAG file for syntax errors

**2. Bronze layer not ingesting data:**
- Verify S3 bucket permissions
- Check Auto Loader configuration
- Review Databricks cluster logs

**3. Silver pipeline failing:**
- Check DLT pipeline logs
- Verify schema compatibility
- Review data quality rules

**4. dbt models failing:**
- Check Databricks connection
- Verify source table existence
- Review model dependencies

**5. Data quality issues:**
- Review monitoring dashboard
- Check quality flags in Silver
- Investigate source data

## Best Practices

### Data Engineering

1. **Idempotency**: All pipelines should be idempotent
2. **Incremental Processing**: Use incremental strategies for large datasets
3. **Data Validation**: Validate at every layer
4. **Documentation**: Document data lineage and transformations
5. **Testing**: Write tests before production deployment

### Code Quality

1. **Version Control**: Use Git for all code
2. **Code Review**: Require PR reviews
3. **Linting**: Use black, flake8 for Python
4. **Type Hints**: Use type hints in Python code
5. **Documentation**: Document complex logic

### Operations

1. **Monitoring**: Monitor all pipelines
2. **Alerting**: Set up alerts for failures
3. **Logging**: Comprehensive logging
4. **Backup**: Regular backups
5. **DR Testing**: Test disaster recovery procedures

## Future Enhancements

### Planned Features

1. **Real-time Processing**: Stream processing with Kafka
2. **Advanced ML**: ML models for prediction
3. **Data Catalog**: Integration with data catalog tools
4. **Advanced Analytics**: More specialized analytics models
5. **API Layer**: REST API for data access
6. **Dashboard**: BI dashboard for stakeholders
7. **Data Quality Automation**: Automated data quality remediation

### Scalability

- **Horizontal Scaling**: Add more Databricks workers
- **Vertical Scaling**: Upgrade instance types
- **Caching**: Implement Redis for metadata
- **CDN**: CloudFront for static assets
- **Read Replicas**: Database read replicas

## Contributing

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make changes and add tests
4. Run local tests
5. Submit a pull request
6. Address review comments
7. Merge after approval

### Code Style

- **Python**: Follow PEP 8
- **SQL**: Follow dbt style guide
- **Documentation**: Update docs with changes

## Support

### Resources

- **NYC Open Data**: https://opendata.cityofnewyork.us/
- **Databricks Docs**: https://docs.databricks.com/
- **dbt Docs**: https://docs.getdbt.com/
- **Airflow Docs**: https://airflow.apache.org/docs/

### Contact

For issues, questions, or contributions, please open an issue in the repository.

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Acknowledgments

- NYC Open Data team for providing the 311 dataset
- Databricks for the Delta Lake framework
- dbt Labs for the dbt framework
- Apache Airflow community
- Contributors and maintainers

---

**Version**: 1.0.0  
**Last Updated**: 2025-01-01  
**Status**: Production-Ready
