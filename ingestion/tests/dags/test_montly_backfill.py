"""
NYC 311 Ingestion - Minimal Critical Tests
Senin çalışan import yapına göre düzenlenmiş versiyon.
"""

import boto3
import pytest
from moto import mock_aws
import requests_mock
import re
from datetime import datetime, timedelta, timezone
from include.nyc311_ingestion import NYC311DataIngestion
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.utils.session import create_session
from unittest.mock import patch, Mock
import uuid

BUCKET = "test-bucket"


@pytest.fixture
def ingestion():
    """Setup NYC311DataIngestion with mocked S3"""
    with mock_aws():
        # Create mocked S3
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        
        # Setup ingestion instance
        ing = NYC311DataIngestion()
        ing.bucket_name = BUCKET
        ing.request_delay = 0
        
        # Mock S3Hook (senin FakeS3Hook yapına benzer)
        class MockS3Hook:
            def check_for_key(self, key, bucket_name):
                try:
                    s3.head_object(Bucket=bucket_name, Key=key)
                    return True
                except:
                    return False
            
            def get_conn(self):
                return s3
            
            def get_key(self, key, bucket_name):
                try:
                    resp = s3.head_object(Bucket=bucket_name, Key=key)
                    obj = Mock()
                    obj.content_length = resp.get('ContentLength', 0)
                    obj.last_modified = resp.get('LastModified')
                    obj.metadata = resp.get('Metadata', {})
                    return obj
                except:
                    return None
        
        ing.s3_hook = MockS3Hook()
        yield ing


# ==================== CRITICAL TESTS ====================

def test_idempotency_check(ingestion):
    """🔥 EN ÖNEMLİ: Dosya varsa skip etmeli (DAG'ın kalbi)"""
    key = "year=2024/month=03/nyc_311_2024_03.parquet"
    
    # File doesn't exist
    assert ingestion.check_file_exists(key) is False
    
    # Upload file
    s3 = ingestion.s3_hook.get_conn()
    s3.put_object(
        Bucket=BUCKET, 
        Key=key, 
        Body=b"test",
        Metadata={"record_count": "100000"}
    )
    
    # Now file exists (idempotency check should return True)
    assert ingestion.check_file_exists(key) is True
    
    # Get metadata (DAG bunu kullanıyor)
    info = ingestion.get_s3_file_info(key)
    assert info["metadata"]["record_count"] == "100000"


def test_parquet_upload_success(ingestion):
    """🔥 Parquet upload çalışıyor mu? (DAG upload_to_s3_parquet kullanıyor)"""
    # Daha gerçekçi veri (file_size_mb > 0 olması için)
    data = [
        {
            "unique_key": str(i),
            "created_date": f"2024-03-15T{i%24:02d}:00:00",
            "complaint_type": "Noise" if i % 2 == 0 else "Water",
            "descriptor": "Test complaint description",
            "status": "Open"
        }
        for i in range(100)
    ]
    
    result = ingestion.upload_to_s3_parquet(
        datetime(2024, 3, 15), 
        data, 
        monthly=True
    )
    
    # Check result structure (DAG bunu XCom'da kullanıyor)
    assert result["status"] == "success"
    assert result["record_count"] == 100
    assert result["format"] == "parquet"
    assert result["compression"] == "snappy"
    assert result["s3_key"] == "year=2024/month=03/nyc_311_2024_03.parquet"
    assert result["file_size_mb"] >= 0  # Küçük veri için 0.0 olabilir
    
    # Verify S3 upload
    s3 = ingestion.s3_hook.get_conn()
    objects = s3.list_objects_v2(Bucket=BUCKET)
    assert objects["KeyCount"] == 1


def test_api_pagination(ingestion):
    """🔥 Pagination doğru çalışıyor mu? (50K+ kayıt varsa)"""
    with requests_mock.Mocker() as m:
        # Mock: 2 sayfa veri (senin regex pattern yapın)
        base_url_pattern = re.compile(r"https://data\.cityofnewyork\.us/resource/erm2-nwe9\.json.*")
        
        page1 = [{"unique_key": str(i)} for i in range(50000)]
        page2 = [{"unique_key": str(i)} for i in range(50000, 60000)]
        
        # First call (offset=0)
        m.get(
            base_url_pattern,
            json=page1,
            additional_matcher=lambda req: "$offset=0" in req.url
        )
        
        # Second call (offset=50000)
        m.get(
            base_url_pattern,
            json=page2,
            additional_matcher=lambda req: "$offset=50000" in req.url
        )
        
        # End of pagination
        m.get(
            base_url_pattern,
            json=[],
            additional_matcher=lambda req: "$offset=100000" in req.url
        )
        
        result = ingestion.fetch_data_for_month(2024, 3)
        
        assert len(result) == 60000


def test_empty_data_handling(ingestion):
    """🔥 Veri yoksa hata vermemeli (validation_monthly_data için)"""
    result = ingestion.upload_to_s3_parquet(
        datetime(2024, 3, 15),
        [],  # Empty data
        monthly=True
    )
    
    # DAG'daki validation bu değerleri kontrol ediyor
    assert result["status"] == "no_data"
    assert result["record_count"] == 0
    assert result["file_size_mb"] == 0


def test_december_edge_case(ingestion):
    """🔥 Aralık ayı yıl rollover'ı (fetch_data_for_month'taki if month == 12)"""
    with requests_mock.Mocker() as m:
        base_url_pattern = re.compile(r"https://data\.cityofnewyork\.us/resource/erm2-nwe9\.json.*")
        
        m.get(
            base_url_pattern,
            json=[{"unique_key": "1"}],
            additional_matcher=lambda req: "$offset=0" in req.url
        )
        m.get(
            base_url_pattern,
            json=[],
            additional_matcher=lambda req: "$offset=50000" in req.url
        )
        
        # Should handle 2024-12 → 2025-01 transition
        result = ingestion.fetch_data_for_month(2024, 12)
        
        assert len(result) == 1
        
        # Check that API was called with correct date range
        history = m.request_history
        assert "2024-12-01" in history[0].url
        assert "2025-01-01" in history[0].url


# ==================== OPTIONAL TESTS ====================

def test_daily_vs_monthly_s3_key(ingestion):
    """Key structure farklı mı? (monthly=True/False)"""
    data = [{"unique_key": "1"}]
    
    # Monthly key
    monthly_result = ingestion.upload_to_s3_parquet(
        datetime(2024, 3, 15), data, monthly=True
    )
    assert monthly_result["s3_key"] == "year=2024/month=03/nyc_311_2024_03.parquet"
    assert "day=" not in monthly_result["s3_key"]
    
    # Daily key
    daily_result = ingestion.upload_to_s3_parquet(
        datetime(2024, 3, 15), data, monthly=False
    )
    assert daily_result["s3_key"] == "year=2024/month=03/day=15/nyc_311_2024_03_15.parquet"
    assert "day=15" in daily_result["s3_key"]


def test_metadata_stored_correctly(ingestion):
    """Metadata S3'e doğru kaydediliyor mu? (DAG log_summary için)"""
    data = [{"unique_key": str(i)} for i in range(100)]
    
    result = ingestion.upload_to_s3_parquet(
        datetime(2024, 3, 15), 
        data, 
        monthly=True
    )
    
    info = ingestion.get_s3_file_info(result["s3_key"])
    
    # DAG'daki validation bu metadata'yı kullanıyor
    assert info["metadata"]["record_count"] == "100"
    assert info["metadata"]["layer"] == "bronze"
    assert info["metadata"]["format"] == "parquet"
    assert info["metadata"]["compression"] == "snappy"


def test_validate_date_range(ingestion):
    """Date validation çalışıyor mu? (class'taki validate_date_range metodu)"""
    # Valid range
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 15)
    assert ingestion.validate_date_range(start, end) is True
    
    # Too long (>32 days)
    end_long = datetime(2024, 3, 1)
    assert ingestion.validate_date_range(start, end_long) is False
    
    # Before 2010
    old_start = datetime(2009, 12, 1)
    assert ingestion.validate_date_range(old_start, end) is False
    
    # Future date
    future_end = datetime.now() + timedelta(days=5)
    assert ingestion.validate_date_range(datetime.now(), future_end) is False