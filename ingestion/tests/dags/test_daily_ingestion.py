"""
NYC 311 Daily Ingestion - Critical Tests
Adapted from monthly test structure for daily DAG.
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
        
        # Mock S3Hook
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

def test_idempotency_check_daily(ingestion):
    """🔥 MOST IMPORTANT: Skip processing if file exists (Heart of Daily DAG)"""
    key = "year=2025/month=10/day=01/nyc_311_2025_10_01.parquet"
    
    # File doesn't exist
    assert ingestion.check_file_exists(key) is False
    
    # Upload file
    s3 = ingestion.s3_hook.get_conn()
    s3.put_object(
        Bucket=BUCKET, 
        Key=key, 
        Body=b"test",
        Metadata={"record_count": "5000"}
    )
    
    # Now file exists (idempotency check should return True)
    assert ingestion.check_file_exists(key) is True
    
    # Get metadata (DAG uses this)
    info = ingestion.get_s3_file_info(key)
    assert info["metadata"]["record_count"] == "5000"


def test_parquet_upload_success_daily(ingestion):
    """🔥 Does daily Parquet upload work correctly?"""
    # Realistic daily data
    data = [
        {
            "unique_key": str(i),
            "created_date": f"2025-10-01T{i%24:02d}:00:00",
            "complaint_type": "Noise" if i % 2 == 0 else "Water",
            "descriptor": "Daily complaint",
            "status": "Open"
        }
        for i in range(100)
    ]
    
    result = ingestion.upload_to_s3_parquet(
        datetime(2025, 10, 1), 
        data, 
        monthly=False  # 🔥 DAILY MODE
    )
    
    # Check result structure (DAG uses this in XCom)
    assert result["status"] == "success"
    assert result["record_count"] == 100
    assert result["format"] == "parquet"
    assert result["compression"] == "snappy"
    assert result["s3_key"] == "year=2025/month=10/day=01/nyc_311_2025_10_01.parquet"
    assert "day=01" in result["s3_key"]  # Daily mode check
    assert result["file_size_mb"] >= 0
    
    # Verify S3 upload
    s3 = ingestion.s3_hook.get_conn()
    objects = s3.list_objects_v2(Bucket=BUCKET)
    assert objects["KeyCount"] == 1


def test_api_fetch_single_day(ingestion):
    """🔥 Single day data fetch (fetch_data_for_date)"""
    with requests_mock.Mocker() as m:
        base_url_pattern = re.compile(r"https://data\.cityofnewyork\.us/resource/erm2-nwe9\.json.*")
        
        # Single day data (1 page is enough for daily)
        daily_data = [{"unique_key": str(i)} for i in range(5000)]
        
        # First call (offset=0)
        m.get(
            base_url_pattern,
            json=daily_data,
            additional_matcher=lambda req: "$offset=0" in req.url
        )
        
        # End of pagination
        m.get(
            base_url_pattern,
            json=[],
            additional_matcher=lambda req: "$offset=50000" in req.url
        )
        
        result = ingestion.fetch_data_for_date(datetime(2025, 10, 1))
        
        assert len(result) == 5000
        
        # Verify date range in API call
        history = m.request_history
        assert "2025-10-01T00:00:00" in history[0].url
        assert "2025-10-02T00:00:00" in history[0].url  # Next day as end


def test_empty_data_handling_daily(ingestion):
    """🔥 Should not error when no daily data exists (for validation)"""
    result = ingestion.upload_to_s3_parquet(
        datetime(2025, 10, 1),
        [],  # Empty data
        monthly=False
    )
    
    # DAG validation checks these values
    assert result["status"] == "no_data"
    assert result["record_count"] == 0
    assert result["file_size_mb"] == 0


def test_daily_s3_key_structure(ingestion):
    """🔥 Is daily key structure correct? (year/month/day)"""
    data = [{"unique_key": "1"}]
    
    # Daily key must have day folder
    result = ingestion.upload_to_s3_parquet(
        datetime(2025, 10, 15), 
        data, 
        monthly=False
    )
    
    expected_key = "year=2025/month=10/day=15/nyc_311_2025_10_15.parquet"
    assert result["s3_key"] == expected_key
    assert "day=15" in result["s3_key"]
    assert "year=2025" in result["s3_key"]
    assert "month=10" in result["s3_key"]


def test_metadata_stored_correctly_daily(ingestion):
    """🔥 Is daily metadata stored correctly in S3?"""
    data = [{"unique_key": str(i)} for i in range(100)]
    
    result = ingestion.upload_to_s3_parquet(
        datetime(2025, 10, 1), 
        data, 
        monthly=False
    )
    
    info = ingestion.get_s3_file_info(result["s3_key"])
    
    # DAG validation uses this metadata
    assert info["metadata"]["record_count"] == "100"
    assert info["metadata"]["layer"] == "bronze"
    assert info["metadata"]["format"] == "parquet"
    assert info["metadata"]["compression"] == "snappy"
    assert info["metadata"]["ingestion_type"] == "daily"


def test_future_date_skip(ingestion):
    """🔥 Future dates should be skipped (DAG skip logic)"""
    future_date = datetime.now() + timedelta(days=5)
    
    # Validation should prevent this
    start = future_date
    end = future_date + timedelta(days=1)
    assert ingestion.validate_date_range(start, end) is False


def test_multiple_daily_runs(ingestion):
    """🔥 Consecutive daily runs (for catchup=True)"""
    dates = [
        datetime(2025, 10, 1),
        datetime(2025, 10, 2),
        datetime(2025, 10, 3),
    ]
    
    for date in dates:
        data = [{"unique_key": f"{date.day}_{i}"} for i in range(100)]
        result = ingestion.upload_to_s3_parquet(date, data, monthly=False)
        assert result["status"] == "success"
    
    # Verify all 3 files exist
    s3 = ingestion.s3_hook.get_conn()
    objects = s3.list_objects_v2(Bucket=BUCKET)
    assert objects["KeyCount"] == 3


# ==================== DAG STRUCTURE TESTS ====================

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_dag_loaded(dagbag):
    """🔥 Is DAG loaded successfully?"""
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    assert dag is not None
    assert len(dag.tasks) > 0


def test_dag_tasks_exist(dagbag):
    """🔥 Do all required tasks exist?"""
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    task_ids = [task.task_id for task in dag.tasks]
    
    assert "start_daily_ingestion" in task_ids
    assert "extract_and_load_daily" in task_ids
    assert "validate_daily_data" in task_ids
    assert "send_daily_summary" in task_ids
    assert "end_daily_ingestion" in task_ids


def test_dag_schedule(dagbag):
    """🔥 Is schedule correct? (9 AM UTC daily)"""
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    # Airflow 2.x uses 'schedule' or 'timetable' instead of 'schedule_interval'
    schedule = getattr(dag, 'schedule', None) or getattr(dag, 'schedule_interval', None)
    assert schedule == "0 9 * * *"


def test_dag_start_date(dagbag):
    """🔥 Is start date November 8, 2025?"""
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    # Compare dates only (ignore timezone differences)
    assert dag.start_date.date() == datetime(2025, 11, 8).date()


def test_dag_catchup_disabled(dagbag):
    """🔥 Is catchup disabled? (for T-2 pattern with manual backfill)"""
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    assert dag.catchup is False


def test_task_dependencies(dagbag):
    """🔥 Is task dependency chain correct?"""
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    
    start = dag.get_task("start_daily_ingestion")
    extract = dag.get_task("extract_and_load_daily")
    validate = dag.get_task("validate_daily_data")
    summary = dag.get_task("send_daily_summary")
    end = dag.get_task("end_daily_ingestion")
    
    # Check dependencies
    assert extract in start.downstream_list
    assert validate in extract.downstream_list
    assert summary in validate.downstream_list
    assert end in summary.downstream_list