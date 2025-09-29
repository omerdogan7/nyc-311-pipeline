import boto3
import pytest
from moto import mock_aws
import requests_mock
import re
from datetime import datetime, timezone
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

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="include/dags", include_examples=False)

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    assert dag is not None
    assert len(dag.tasks) > 0

def test_dag_run(dagbag, s3_setup, requests_mock, session):
    ingestion, s3 = s3_setup  # Get the patched ingestion instance
    dag = dagbag.get_dag(dag_id="nyc311_daily_ingestion")
    logical_date = datetime.now(timezone.utc)
    run_id = f"test__{logical_date.isoformat()}_{uuid.uuid4().hex[:8]}"

    # API mock
    base_url_pattern = re.compile(r"https://data\.cityofnewyork\.us/resource/erm2-nwe9\.json.*")
    requests_mock.get(
        base_url_pattern,
        json=[{
            "unique_key": "12345",
            "created_date": "2025-09-17T10:00:00.000",
            "complaint_type": "Noise",
            "descriptor": "Loud Music",
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