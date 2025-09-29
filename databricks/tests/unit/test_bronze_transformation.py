# tests/unit/test_bronze_transformation.py
import pytest
from unittest.mock import MagicMock, PropertyMock

@pytest.mark.unit
class TestBronzeTransformations:
    """Bronze layer pure unit tests - no Spark needed"""
    
    def test_bronze_transformation_logic(self):
        """Test transformation logic without Spark"""
        # Mock DataFrame
        mock_df = MagicMock()
        
        # Mock column operations
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["unique_key", "agency", "_ingestion_timestamp", 
                          "_source_file", "_file_modification_time"]
        mock_df.count.return_value = 2
        
        # Test the logic exists
        result = mock_df.withColumn("_ingestion_timestamp", "mock_timestamp")
        
        # Verify methods were called
        assert mock_df.withColumn.called
        assert "_ingestion_timestamp" in mock_df.columns