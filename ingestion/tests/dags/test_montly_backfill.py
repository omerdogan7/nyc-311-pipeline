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
from unittest.mock import patch
import uuid

BUCKET = "test-bucket"

@pytest.fixture
def session():
    """SQLAlchemy session for DAG/TaskInstance tests."""
    with create_session() as session:
        yield session

@pytest.fixture
def s3_setup():
    """Fake S3 client (moto ile) ve FakeS3Hook"""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)

        ingestion = NYC311DataIngestion()

        class FakeS3Hook:
            def check_for_key(self, key, bucket_name):
                try:
                    s3.head_object(Bucket=bucket_name, Key=key)
                    return True
                except Exception:
                    return False

            def get_conn(self):
                """Return the mocked S3 client directly"""
                return s3

            def load_bytes(self, bytes_data, key, bucket_name, replace=True, **kwargs):
                """Mock S3Hook.load_bytes method"""
                put_params = {
                    'Bucket': bucket_name,
                    'Key': key,
                    'Body': bytes_data
                }
                s3.put_object(**put_params)

            def get_key(self, key, bucket_name):
                """Mock get_key method for S3 object info"""
                from unittest.mock import Mock
                try:
                    response = s3.head_object(Bucket=bucket_name, Key=key)
                    mock_obj = Mock()
                    mock_obj.content_length = response.get('ContentLength', 0)
                    mock_obj.last_modified = response.get('LastModified')
                    mock_obj.metadata = response.get('Metadata', {})
                    return mock_obj
                except Exception:
                    return None

            def load_string(self, string_data, key, bucket_name, replace=True, encoding='utf-8', **kwargs):
                """Handle string uploads if needed"""
                self.load_bytes(string_data.encode(encoding), key, bucket_name, replace, **kwargs)
        
        ingestion.s3_hook = FakeS3Hook()
        ingestion.bucket_name = BUCKET
        ingestion.request_delay = 0

        yield ingestion, s3


def test_check_file_exists(s3_setup):
    ingestion, s3 = s3_setup
    key = "year=2025/month=09/day=01/test.json.gz"
    assert ingestion.check_file_exists(key) is False
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"data")
    assert ingestion.check_file_exists(key) is True


def test_monthly_ingestion(requests_mock, s3_setup):
    ingestion, s3 = s3_setup
    year, month = 2025, 8
    fake_data = [
        {
            "unique_key": "67890",
            "created_date": "2025-08-01T10:00:00.000",
            "complaint_type": "Water",
            "descriptor": "Pipe leak",
            "status": "Open"
        }
    ]

    base_url_pattern = re.compile(r"https://data\.cityofnewyork\.us/resource/erm2-nwe9\.json.*")
    requests_mock.get(base_url_pattern, json=fake_data, additional_matcher=lambda req: "$offset=0" in req.url)
    requests_mock.get(base_url_pattern, json=[], additional_matcher=lambda req: "$offset=0" not in req.url)

    test_date = datetime(year, month, 1)
    data = ingestion.fetch_data_for_month(year, month)
    info = ingestion.upload_to_s3(test_date, data, monthly=True)

    expected_key = f"year={year}/month={month:02d}/nyc_311_{year}_{month:02d}.json.gz"
    result = s3.list_objects_v2(Bucket=BUCKET)
    keys = [obj["Key"] for obj in result.get("Contents", [])]

    assert data == fake_data
    assert info["status"] == "success"
    assert len(keys) == 1
    assert keys[0] == expected_key


def test_upload_to_s3_no_data(s3_setup):
    ingestion, _ = s3_setup
    result = ingestion.upload_to_s3(datetime(2025, 8, 1), [], monthly=True)
    assert result["status"] == "no_data"


def test_validate_date_range_rules(s3_setup):
    ingestion, _ = s3_setup
    start = datetime(2025, 8, 1)
    end = datetime(2025, 8, 15)
    assert ingestion.validate_date_range(start, end) is True
    invalid_end = datetime(2025, 9, 30)
    assert ingestion.validate_date_range(start, invalid_end) is False
    assert ingestion.validate_date_range(datetime(2000, 1, 1), end) is False
    future = datetime.now() + timedelta(days=5)
    assert ingestion.validate_date_range(datetime.now(), future) is False


# ---------------- DAG TESTLERI ----------------

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="include/dags", include_examples=False)

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="nyc311_monthly_backfill")
    assert dag is not None
    assert len(dag.tasks) > 0

def test_dag_run(dagbag, s3_setup, requests_mock, session):
    ingestion, s3 = s3_setup  # Get the patched ingestion instance
    dag = dagbag.get_dag(dag_id="nyc311_monthly_backfill")
    logical_date = datetime.now(timezone.utc)
    run_id = f"test__{logical_date.isoformat()}_{uuid.uuid4().hex[:8]}"

    # API mock
    base_url_pattern = re.compile(r"https://data\.cityofnewyork\.us/resource/erm2-nwe9\.json.*")
    requests_mock.get(
        base_url_pattern,
        json=[{
            "unique_key": "12345",
            "created_date": "2025-08-01T10:00:00.000",
            "complaint_type": "Noise",
            "descriptor": "Loud music",
            "status": "Open"
        }],
        additional_matcher=lambda req: "$offset=0" in req.url
    )
    requests_mock.get(
        base_url_pattern,
        json=[],
        additional_matcher=lambda req: "$offset=0" not in req.url
    )

    # Also patch the Variable.get calls that might be used in the DAG
    with patch('airflow.models.Variable.get') as mock_var_get:
        def mock_get(key, default_var=None, **kwargs):
            if key == "nyc_311_bucket":
                return BUCKET
            elif key == "nyc_311_request_delay":
                return "0"
            return default_var
        
        mock_var_get.side_effect = mock_get

        # DAG run oluştur
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_id=run_id,
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            logical_date=logical_date,
            start_date=datetime.now(timezone.utc)
        )
        session.add(dag_run)
        session.commit()

        # TaskInstance çalıştır
        for task in dag.tasks:
            ti = TaskInstance(task=task, run_id=dag_run.run_id)
            ti.run(ignore_ti_state=True, ignore_all_deps=True, session=session)