import pytest
from pyspark.sql import SparkSession
from resources.src.setup.setup_catalog import InfrastructureConfig, InfrastructureSetup


@pytest.mark.integration
class TestCatalogSetupIntegration:
    """Gerçek Spark ile basit integration testler"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Test için gerçek Spark session"""
        return SparkSession.builder \
            .appName("test_setup") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    @pytest.fixture
    def test_setup(self, spark):
        """Test için setup instance"""
        config = InfrastructureConfig(
            catalog_name="test_catalog_int",
            bronze_bucket="s3://fake-bronze",  # Gerçek S3 gerekmez
            data_bucket="s3://fake-data",
            environment="test"
        )
        setup = InfrastructureSetup(config, spark)
        
        # Volume creation'ları atla (S3 credential gerektirir)
        setup.create_external_volumes = lambda: None
        setup.set_permissions = lambda: None  # Permission hataları önle
        
        yield setup
        
        # Cleanup
        try:
            spark.sql("DROP CATALOG IF EXISTS test_catalog_int CASCADE")
        except:
            pass
    
    def test_create_catalog_and_schemas(self, test_setup):
        """Catalog ve schema gerçekten oluşuyor mu?"""
        # Sadece catalog ve schema oluştur
        test_setup.create_catalog()
        test_setup.create_schemas()
        
        # Gerçekten oluştu mu kontrol et
        catalogs = test_setup.spark.sql("SHOW CATALOGS").collect()
        assert any(c.catalog == "test_catalog_int" for c in catalogs)
        
        # Schema'lar var mı?
        test_setup.spark.sql("USE CATALOG test_catalog_int")
        schemas = test_setup.spark.sql("SHOW SCHEMAS").collect()
        schema_names = [s.namespace for s in schemas]
        
        assert "bronze" in schema_names
        assert "silver" in schema_names
        assert "gold" in schema_names
    
    def test_full_setup_without_volumes(self, test_setup):
        """Volume'sız tam setup testi"""
        # Setup'ı çalıştır
        test_setup.setup()
        
        # En az catalog ve schema'lar oluşmuş olmalı
        assert len(test_setup.created_resources) >= 5  # 1 catalog + 4 schema
        
        # Verify çalışıyor mu?
        test_setup.verify_setup()  # Hata vermemeli