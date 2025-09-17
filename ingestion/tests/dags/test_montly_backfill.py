import boto3
import pytest
from moto import mock_aws
import requests_mock
import re
from datetime import datetime, timedelta
from include.nyc311_ingestion import NYC311DataIngestion

BUCKET = "test-bucket"


@pytest.fixture
def s3_setup():
    """Fake S3 client (moto ile) ve mock S3Hook"""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)

        ingestion = NYC311DataIngestion()

        # Fake S3Hook tanımı
        class FakeS3Hook:
            def check_for_key(self, key, bucket_name):
                try:
                    s3.head_object(Bucket=bucket_name, Key=key)
                    return True
                except Exception:
                    return False

            def load_bytes(self, bytes_data, key, bucket_name, replace=True, metadata=None):
                s3.put_object(Bucket=bucket_name, Key=key, Body=bytes_data, Metadata=metadata or {})

            def get_key(self, key, bucket_name):
                obj = s3.get_object(Bucket=bucket_name, Key=key)
                return type("Obj", (), {
                    "content_length": obj["ContentLength"],
                    "last_modified": obj["LastModified"],
                    "metadata": obj["Metadata"]
                })

        ingestion.s3_hook = FakeS3Hook()
        ingestion.bucket_name = BUCKET

        # Test için rate limit sıfırlansın
        ingestion.request_delay = 0

        yield ingestion, s3


def test_check_file_exists(s3_setup):
    ingestion, s3 = s3_setup
    key = "year=2025/month=09/day=01/test.json.gz"

    # başta yok
    assert ingestion.check_file_exists(key) is False

    # ekle
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"data")
    assert ingestion.check_file_exists(key) is True


def test_monthly_ingestion(requests_mock, s3_setup):
    """Monthly ingestion test"""
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

    # offset=0 için veri döndür
    requests_mock.get(
        base_url_pattern,
        json=fake_data,
        additional_matcher=lambda req: "$offset=0" in req.url
    )

    # diğer offsetler boş liste döndürsün (pagination sonlandır)
    requests_mock.get(
        base_url_pattern,
        json=[],
        additional_matcher=lambda req: "$offset=0" not in req.url
    )

    test_date = datetime(year, month, 1)

    print("Fetching monthly data...")
    data = ingestion.fetch_data_for_month(year, month)
    print(f"Fetched monthly data: {data}")

    print("Uploading to S3...")
    info = ingestion.upload_to_s3(test_date, data, monthly=True)
    print(f"Upload info: {info}")

    expected_key = f"year={year}/month={month:02d}/nyc_311_{year}_{month:02d}.json.gz"
    result = s3.list_objects_v2(Bucket=BUCKET)
    keys = [obj["Key"] for obj in result.get("Contents", [])]

    # Assertions
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

    # geçerli
    assert ingestion.validate_date_range(start, end) is True

    # çok uzun aralık
    invalid_end = datetime(2025, 9, 30)
    assert ingestion.validate_date_range(start, invalid_end) is False

    # çok eski tarih
    assert ingestion.validate_date_range(datetime(2000, 1, 1), end) is False

    # gelecek tarih
    future = datetime.now() + timedelta(days=5)
    assert ingestion.validate_date_range(datetime.now(), future) is False
