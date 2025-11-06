import requests
import json
import gzip
import polars as pl
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from io import BytesIO
import time
import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

class NYC311DataIngestion:
    """NYC 311 data ingestion utilities for Astro/Airflow (s3 upload, API fetch, etc.)"""
    
    def __init__(self):
        self.base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
        self.limit = 50000
        self.bucket_name = Variable.get("nyc_311_bucket", default_var="nyc-311-raw")
        self.s3_hook = S3Hook(aws_conn_id='aws_default')
        # Optional: Configurable rate limiting
        self.request_delay = 0.2  # seconds between requests
        
    def check_file_exists(self, s3_key: str) -> bool:
        """Check if the file already exists in S3"""
        try:
            return self.s3_hook.check_for_key(key=s3_key, bucket_name=self.bucket_name)
        except Exception as e:
            logger.warning(f"Error checking S3 key existence: {e}")
            return False

    def fetch_data_for_date(self, target_date: datetime) -> List[Dict]:
        """Get data for a specific date (daily) from NYC 311 API"""
        start_str = target_date.strftime("%Y-%m-%dT00:00:00")
        end_str = (target_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        return self._fetch_data(start_str, end_str, f"daily-{target_date.date()}")

    def fetch_data_for_month(self, year: int, month: int) -> List[Dict]:
        """Get data for a specific month (monthly) from NYC 311 API"""
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        start_str = start_date.strftime("%Y-%m-%dT00:00:00")
        end_str = end_date.strftime("%Y-%m-%dT00:00:00")
        return self._fetch_data(start_str, end_str, f"monthly-{year}-{month:02d}")

    def _fetch_data(self, start_str: str, end_str: str, context: str = "") -> List[Dict]:
        """Internal method to fetch data from API with pagination"""
        all_data = []
        offset = 0
        request_count = 0
        start_time = time.time()

        logger.info(f"🔄 [{context}] Fetching data from {start_str} to {end_str}")

        while True:
            url = f"{self.base_url}?$where=created_date >= '{start_str}' AND created_date < '{end_str}'&$limit={self.limit}&$offset={offset}&$order=created_date"

            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                batch = response.json()
                request_count += 1

                if not batch:
                    elapsed = time.time() - start_time
                    logger.info(f"✅ [{context}] Completed after {request_count} API calls in {elapsed:.1f}s")
                    break

                all_data.extend(batch)
                offset += self.limit

                # Configurable rate limiting
                if self.request_delay > 0:
                    time.sleep(self.request_delay)

                # Progress logging every 5 requests for large datasets
                if len(batch) == self.limit and request_count % 5 == 0:
                    logger.info(f"📦 [{context}] Batch {request_count}: {len(batch)} records, total: {len(all_data)}")

            except requests.exceptions.RequestException as e:
                logger.error(f"❌ [{context}] API error: {e}")
                if request_count > 5:  # Prevent infinite retry loop
                    raise
                time.sleep(30)  # Wait before retry
                continue

        logger.info(f"✅ [{context}] Total records fetched: {len(all_data)} in {request_count} requests")
        return all_data

    def upload_to_s3_parquet(self, target_date: datetime, data: List[Dict], monthly: bool = False) -> Dict:
        """Upload RAW data to S3 in Parquet format (Bronze layer - no transformations)"""
        
        # Generate S3 key based on monthly/daily mode
        if monthly:
            s3_key = f"year={target_date.year}/month={target_date.month:02d}/nyc_311_{target_date.year}_{target_date.month:02d}.parquet"
            context = f"monthly-{target_date.year}-{target_date.month:02d}"
        else:
            s3_key = f"year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/nyc_311_{target_date.strftime('%Y_%m_%d')}.parquet"
            context = f"daily-{target_date.date()}"

        # Handle empty data case
        if not data:
            logger.info(f"⚠️ [{context}] No data to upload")
            return {
                "status": "no_data",
                "s3_key": s3_key,
                "record_count": 0,
                "file_size_mb": 0,
                "context": context
            }

        try:
            # Convert RAW JSON to Polars DataFrame (no transformations)
            logger.info(f"🔄 [{context}] Converting {len(data):,} records to Parquet...")
            df = pl.DataFrame(data)
            
            # Write to memory buffer
            buffer = BytesIO()
            df.write_parquet(
                buffer,
                compression='snappy',  # Fast compression
                use_pyarrow=True
            )
            
            # Get buffer data
            buffer.seek(0)
            parquet_data = buffer.getvalue()
            file_size_mb = len(parquet_data) / (1024 * 1024)
            
            # Metadata for S3
            metadata = {
                'record_count': str(len(df)),
                'column_count': str(len(df.columns)),
                'ingestion_timestamp': datetime.now().isoformat(),
                'source': 'nyc-opendata-api',
                'format': 'parquet',
                'compression': 'snappy',
                'ingestion_type': 'monthly' if monthly else 'daily',
                'layer': 'bronze',
                'data_quality': 'raw',
                'file_size_mb': str(round(file_size_mb, 2))
            }
            
            # Upload to S3
            s3_client = self.s3_hook.get_conn()
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_data,
                Metadata=metadata,
                ContentType='application/octet-stream'
            )
            
            logger.info(f"📤 [{context}] Uploaded RAW Parquet to S3: {s3_key}")
            logger.info(f"   Records: {len(df):,} | Columns: {len(df.columns)} | Size: {file_size_mb:.2f} MB")
            
            return {
                "status": "success",
                "s3_key": s3_key,
                "record_count": len(df),
                "column_count": len(df.columns),
                "file_size_mb": round(file_size_mb, 2),
                "format": "parquet",
                "compression": "snappy",
                "context": context
            }
            
        except Exception as e:
            logger.error(f"❌ [{context}] Parquet upload failed: {e}")
            raise

    def upload_to_s3(self, target_date: datetime, data: List[Dict], monthly: bool = False) -> Dict:
        """Upload the data to S3 in compressed JSON format (Legacy method)"""
        # Generate S3 key based on monthly/daily mode
        if monthly:
            s3_key = f"year={target_date.year}/month={target_date.month:02d}/nyc_311_{target_date.year}_{target_date.month:02d}.json.gz"
            context = f"monthly-{target_date.year}-{target_date.month:02d}"
        else:
            s3_key = f"year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/nyc_311_{target_date.strftime('%Y_%m_%d')}.json.gz"
            context = f"daily-{target_date.date()}"

        # Handle empty data case
        if not data:
            logger.info(f"⚠️ [{context}] No data to upload")
            return {
                "status": "no_data",
                "s3_key": s3_key,
                "record_count": 0,
                "file_size_mb": 0,
                "context": context
            }

        try:
            # Convert to JSON and compress
            json_data = json.dumps(data, default=str, separators=(',', ':'))
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            file_size_mb = len(compressed_data) / (1024 * 1024)

            # Enhanced metadata
            metadata = {
                'record_count': str(len(data)),
                'ingestion_timestamp': datetime.now().isoformat(),
                'source': 'nyc-opendata-api',
                'content_encoding': 'gzip',
                'ingestion_type': 'monthly' if monthly else 'daily',
                'original_size_mb': str(round(len(json_data) / (1024 * 1024), 2)),
                'compressed_size_mb': str(round(file_size_mb, 2))
            }

            # Use S3Hook's boto3 client directly to support metadata
            s3_client = self.s3_hook.get_conn()
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                Metadata=metadata,
                ContentType='application/json',
                ContentEncoding='gzip'
            )

            logger.info(f"📤 [{context}] Uploaded {s3_key}: {len(data)} records, {file_size_mb:.2f} MB")

            return {
                "status": "success",
                "s3_key": s3_key,
                "record_count": len(data),
                "file_size_mb": round(file_size_mb, 2),
                "compression_ratio": round(len(json_data) / len(compressed_data), 2),
                "context": context
            }

        except Exception as e:
            logger.error(f"❌ [{context}] S3 upload failed: {e}")
            raise
            
    def get_s3_file_info(self, s3_key: str) -> Optional[Dict]:
        """Get metadata information about an S3 file (useful for monitoring)"""
        try:
            if not self.check_file_exists(s3_key):
                return None
                
            obj = self.s3_hook.get_key(key=s3_key, bucket_name=self.bucket_name)
            return {
                "key": s3_key,
                "size_bytes": obj.content_length,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                "metadata": obj.metadata or {}
            }
        except Exception as e:
            logger.error(f"Error getting S3 file info for {s3_key}: {e}")
            return None

    def validate_date_range(self, start_date: datetime, end_date: datetime) -> bool:
        """Validate date range for data fetching"""
        # Don't allow future dates beyond today + 1 day
        max_date = datetime.now() + timedelta(days=1)
        if end_date > max_date:
            logger.warning(f"End date {end_date.date()} is beyond allowed range")
            return False
            
        # Don't allow ranges too far in the past (optional business rule)
        min_date = datetime(2010, 1, 1)
        if start_date < min_date:
            logger.warning(f"Start date {start_date.date()} is before minimum allowed date")
            return False
            
        # Don't allow ranges longer than 32 days for single fetch
        if (end_date - start_date).days > 32:
            logger.warning(f"Date range too large: {(end_date - start_date).days} days")
            return False
            
        return True