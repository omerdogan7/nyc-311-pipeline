import pytest
from unittest.mock import MagicMock
from resources.src.setup.setup_catalog import InfrastructureConfig, InfrastructureSetup


def test_config_validation():
    """Config validation çalışıyor mu?"""
    # Geçerli config
    config = InfrastructureConfig(
        catalog_name="test",
        bronze_bucket="s3://bronze",
        data_bucket="s3://data"
    )
    config.validate()  # Hata vermemeli
    
    # Geçersiz config
    config.catalog_name = ""
    with pytest.raises(ValueError):
        config.validate()


def test_setup_creates_resources():
    """Setup resource'ları oluşturuyor mu?"""
    config = InfrastructureConfig(
        catalog_name="test",
        bronze_bucket="s3://bronze",
        data_bucket="s3://data"
    )
    mock_spark = MagicMock()
    setup = InfrastructureSetup(config, mock_spark)
    
    # Catalog oluştur
    setup.create_catalog()
    
    # SQL çağrıldı mı ve resource eklendi mi?
    assert mock_spark.sql.called
    assert len(setup.created_resources) > 0


def test_rollback_works():
    """Rollback çalışıyor mu?"""
    config = InfrastructureConfig(
        catalog_name="test",
        bronze_bucket="s3://bronze",
        data_bucket="s3://data"
    )
    mock_spark = MagicMock()
    setup = InfrastructureSetup(config, mock_spark)
    
    # Resource ekle ve rollback yap
    setup.created_resources = [("SCHEMA", "test.bronze")]
    setup.rollback()
    
    # DROP komutu çağrıldı mı?
    assert any("DROP" in str(call) for call in mock_spark.sql.call_args_list)