![Tech Stack Animation](/assets/apache_airflow.png)

[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-FF6B35?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![AWS S3](https://img.shields.io/badge/AWS_S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)


# NYC 311 Ingestion

Apache Airflow orchestration for NYC 311 data ingestion to AWS S3 using Astronomer Astro CLI.

## Overview

Extracts NYC 311 service request data from NYC Open Data API, converts to Parquet format using Polars, and stores in S3 as a Bronze layer (raw data, no transformations).

**Data Flow**: API (JSON) → Polars DataFrame → Parquet (Snappy) → S3

## Quick Start

```bash
# Start Airflow locally
astro dev start

# Access UI: http://localhost:8080
# Username: admin / Password: admin

# Stop Airflow
astro dev stop
```

## Project Structure

```
ingestion/
├── dags/
│   ├── nyc311_daily_ingestion.py       # Daily incremental load
│   └── nyc311_historical_backfill.py   # Monthly backfill (2010-2025)
├── include/
│   └── nyc311_ingestion.py             # Core logic: API fetch, Parquet conversion, S3 upload
├── tests/dags/                         # DAG validation tests
├── requirements.txt                    # Python dependencies (boto3, polars, pyarrow)
└── Dockerfile                          # Astro Runtime image
```

## DAGs

### 1. Daily Ingestion (`nyc311_daily_ingestion`)

**Purpose**: Incremental daily data extraction

- **Schedule**: 9 AM UTC (catchup enabled for recent days)
- **Date Range**: Previous day's data (00:00 - 23:59)
- **Output Path**: `s3://bucket/year=YYYY/month=MM/day=DD/nyc_311_YYYY_MM_DD.parquet`
- **Features**:
  - Idempotent (skips if file exists)
  - SoQL queries with pagination (50K records/request)
  - Polars for efficient data transformation
  - Data validation after upload

**Example**: Run on 2025-11-03 → fetches data for 2025-11-02

### 2. Historical Backfill (`nyc311_monthly_backfill_idempotent`)

**Purpose**: Backfill historical data in monthly batches

- **Schedule**: Monthly with catchup (2010-01 to 2025-09)
- **Batch Size**: One month per DAG run
- **Output Path**: `s3://bucket/year=YYYY/month=MM/nyc_311_YYYY_MM.parquet`
- **Features**:
  - Idempotent S3 checks (skips existing months)
  - Handles large datasets (~300K records/month)
  - Record count validation (50K-350K range)
  - Parallel execution (max 4 active runs)

**Example**: 2023-05 run → fetches all May 2023 data as single Parquet file

**Note**: Date range can be adjusted in DAG file:
```python
start_date=datetime(2010, 1, 1),   # Adjust start date
end_date=datetime(2025, 10, 31),   # Adjust end date (September was used for initial run)
```

## How It Works

### Data Extraction
1. **API Call**: Query NYC Open Data API with SoQL filters
   ```python
   created_date >= '2025-11-02T00:00:00' AND created_date < '2025-11-03T00:00:00'
   ```
2. **Pagination**: Fetch in 50K record batches with offset/limit
3. **Rate Limiting**: 0.2s delay between requests to respect API limits

### Data Processing
1. **Convert to Polars**: Raw JSON → Polars DataFrame
   ```python
   df = pl.DataFrame(data)  # No transformations, raw data preserved
   ```
2. **Write Parquet**: In-memory conversion with Snappy compression
   ```python
   df.write_parquet(buffer, compression='snappy')
   ```
3. **Upload S3**: Boto3 with metadata (record count, timestamps, etc.)

### Idempotency
Both DAGs check S3 before processing:
```python
if file_exists(s3_key):
    return {"status": "skipped", "reason": "file_exists"}
```

## Configuration

Create `.env` file in project root:

```bash
# NYC Open Data API Token (get from: https://data.cityofnewyork.us/)
NYC_311_APP_TOKEN=your_app_token_here

# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1

# S3 Bucket
S3_BUCKET_NAME=nyc-311-raw
```

**Airflow Variables** (set in UI or via CLI):
- `nyc_311_bucket`: S3 bucket name (default: `nyc-311-bronze`)

**AWS Connection** (set in Airflow UI):
- Connection ID: `aws_default`
- Type: Amazon Web Services
- Extra: `{"region_name": "us-east-1"}`

## Testing

Tests validate DAG structure and imports:

```bash
# Start Airflow and enter container
astro dev bash

# Inside container - run DAG tests
pytest tests/dags/

# Validate DAG imports manually
python dags/nyc311_daily_ingestion.py
python dags/nyc311_historical_backfill.py
```

## Data Format

### Parquet Schema
- **Compression**: Snappy (fast compression for analytics)
- **Engine**: PyArrow
- **Layer**: Bronze (raw data, no cleaning or transformations)
- **Columns**: All original API fields preserved

### S3 Metadata
Each Parquet file includes metadata:
```json
{
  "record_count": "12543",
  "ingestion_timestamp": "2025-11-03T09:15:23",
  "format": "parquet",
  "compression": "snappy",
  "layer": "bronze",
  "source": "nyc-opendata-api"
}
```

### Partitioning Strategy
```
Daily:   year=2025/month=11/day=03/nyc_311_2025_11_03.parquet
Monthly: year=2023/month=05/nyc_311_2023_05.parquet
```

## Monitoring

### Airflow UI
- **DAG Runs**: Monitor execution status and duration
- **Task Logs**: View detailed logs for each task
- **XCom**: Check task outputs (record counts, S3 keys)

### Log Messages
```
✅ [daily-2025-11-02] Extraction completed: 12,543 records
📤 [monthly-2023-05] Uploaded RAW Parquet to S3: year=2023/month=05/...
⏭️ [monthly-2023-06] File exists, skipping (records: 145,892)
```

## Troubleshooting

### DAG not appearing
```bash
# Check syntax
python dags/nyc311_daily_ingestion.py

# Restart scheduler
astro dev restart
```

### API rate limits
- **With token**: 1000 requests/hour
- **Solution**: Built-in 0.2s delay between requests

### S3 permission errors
- Verify IAM permissions: `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`
- Check bucket exists in correct region

## Resources

- [Astronomer Docs](https://docs.astronomer.io/)
- [NYC Open Data API](https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9)
- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Apache Airflow](https://airflow.apache.org/docs/)


## 📄 License
MIT License 

---

**Version**: 1.0  
**Last Updated**: November 2025  
