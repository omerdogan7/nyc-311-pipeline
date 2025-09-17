import pytest
import boto3
from moto import mock_aws
import requests_mock
from datetime import datetime, timedelta
from include.nyc311_ingestion import NYC311DataIngestion
import re

BUCKET = "test-bucket"


@pytest.fixture
def s3_setup():
    """Fake S3 client (moto ile) + FakeS3Hook"""
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

            def load_bytes(self, bytes_data, key, bucket_name, replace=True, metadata=None):
                s3.put_object(Bucket=bucket_name, Key=key, Body=bytes_data, Metadata=metadata or {})

        ingestion.s3_hook = FakeS3Hook()
        ingestion.bucket_name = BUCKET

        # Test için rate limit sıfırlansın
        ingestion.request_delay = 0

        yield ingestion, s3

def test_daily_ingestion(requests_mock, s3_setup):
    fake_data = [
        {
            "unique_key": "12345",
            "created_date": "2025-09-17T10:00:00.000",
            "complaint_type": "Noise",
            "descriptor": "Loud Music",
            "status": "Open"
        }
    ]

    ingestion, s3 = s3_setup
    test_date = datetime(2025, 9, 17)

    base_url_pattern = re.compile(r"https://data\.cityofnewyork\.us/resource/erm2-nwe9\.json.*")

    def url_matcher(request):
        return (
            "created_date" in request.url
            and "2025-09-17" in request.url
            and "$limit=50000" in request.url
            and "$order=created_date" in request.url
        )

        # python
    # return data only for offset=0
    requests_mock.get(
        base_url_pattern,
        json=fake_data,
        additional_matcher=lambda req: "$offset=0" in req.url
    )
    
    # return empty list for any other offset (ends pagination)
    requests_mock.get(
        base_url_pattern,
        json=[],
        additional_matcher=lambda req: "$offset=0" not in req.url
    )
        # python (add at end of your test)
    print("mock request history:", [r.url for r in requests_mock.request_history])
    expected_key = f"year=2025/month=09/day=17/nyc_311_2025_09_17.json.gz"

    try:
        # Fetch işlemi
        print("Fetching data...")  # Debug için
        data = ingestion.fetch_data_for_date(test_date)
        print(f"Fetched data: {data}")  # Debug için
        assert data == fake_data, "API'den gelen veri beklenen ile eşleşmiyor"

                # Upload işlemi
        print("Uploading to S3...")  # Debug için
        info = ingestion.upload_to_s3(test_date, data, monthly=False)
        print(f"Upload info: {info}")  # Debug için

        assert info["status"] == "success"
        
        
        # S3 kontrolleri
        result = s3.list_objects_v2(Bucket=BUCKET)
        assert "Contents" in result, "S3'te dosya bulunamadı"
        
        keys = [obj["Key"] for obj in result.get("Contents", [])]
        assert len(keys) == 1, f"Beklenen: 1 dosya, Bulunan: {len(keys)} dosya"
        assert keys[0] == expected_key, f"Beklenen anahtar: {expected_key}, Bulunan: {keys[0]}"
        
    except Exception as e:
        print(f"Hata oluştu: {str(e)}")  # Debug için
        raise



def test_upload_daily_file_overwrite(s3_setup):
    """Dosya zaten varsa overwrite ediliyor mu?"""
    ingestion, s3 = s3_setup
    key = "year=2025/month=09/day=17/nyc_311_2025_09_17.json.gz"

    # İlk yükleme
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"exists")

    # Tekrar upload et
    fake_data = [{"id": 1}]
    ingestion.upload_to_s3(datetime(2025, 9, 17), fake_data, monthly=False)

    # Dosya overwrite edildi mi?
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"].read()
    assert b"exists" not in body  # eski içerik olmamalı


def test_validate_date_range():
    """Geçerli ve geçersiz tarih aralıklarını test et"""
    ingestion = NYC311DataIngestion()
    today = datetime.now()

    # Geçerli: son 7 gün
    assert ingestion.validate_date_range(today - timedelta(days=7), today)

    # Geçersiz: 2010'dan önce
    assert ingestion.validate_date_range(datetime(2000, 1, 1), today) is False

    # Geçersiz: çok ileri tarih
    future = today + timedelta(days=10)
    assert ingestion.validate_date_range(today, future) is False

def test_upload_empty_data(s3_setup):
    ingestion, s3 = s3_setup
    target_date = datetime(2025, 9, 17)
    result = ingestion.upload_to_s3(target_date, [], monthly=False)
    assert result["status"] == "no_data"
    assert result["record_count"] == 0

def test_get_s3_file_info_nonexistent(s3_setup):
    ingestion, s3 = s3_setup
    info = ingestion.get_s3_file_info("nonexistent_key.json.gz")
    assert info is None

def test_check_file_exists_false(s3_setup):
    ingestion, s3 = s3_setup
    assert ingestion.check_file_exists("nonexistent_key.json.gz") is False

def test_validate_date_range_too_long():
    ingestion = NYC311DataIngestion()
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 2, 10)  # >32 gün
    assert ingestion.validate_date_range(start_date, end_date) is False
