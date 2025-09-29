import dlt
from pyspark.sql.functions import col, current_timestamp, to_timestamp, datediff, when

# Main silver table with incremental loading
@dlt.table(
    name="nyc_311_silver",
    comment="Final silver table for NYC 311 service requests optimized for 15 years of data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "agency,borough,complaint_type,created_date",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.targetFileSize": "128MB",
        "delta.logRetentionDuration": "30 days",
        "delta.deletedFileRetentionDuration": "7 days"
    },
    partition_cols=["created_year", "created_month"]
)
@dlt.expect_or_fail("unique_key_not_null", "unique_key IS NOT NULL")
@dlt.expect("valid_created_date", "created_date IS NOT NULL")
def silver_final():
    df = dlt.read_stream("silver_enriched_311")

    # Select and order final columns
    final_columns = [
        # Core identifiers and dates
        "unique_key", "created_date", "closed_date", 
        
        # Agency and location
        "agency", "agency_name", "borough", "city",
        
        # Complaint details
        "complaint_type", "descriptor", "status",
        
        # Address information
        "incident_address", "street_name", "cross_street_1", "cross_street_2",
        "intersection_street_1", "intersection_street_2", "landmark",
        "location_type", "community_board", "incident_zip",
        
        # Geographic coordinates
        "latitude", "longitude", "has_valid_location",
        
        # Resolution information
        "resolution_description", "resolution_action_updated_date",
        "detailed_outcome_category", "main_outcome_category",
        
        # Analysis flags and metrics
        "was_inspected", "violation_issued", "requires_follow_up",
        "has_resolution", "is_closed",
        
        # Response time metrics
        "response_time_hours", "response_time_days", "response_sla_category",
        
        # Temporal dimensions (partition and clustering)
        "created_year", "created_month", "created_dayofweek", "created_hour",
        "closed_year", "closed_month",
        
        # Temporal flags
        "is_business_hours", "is_weekend",
        
        # Address classification
        "address_type"
    ]

    missing_cols = set(final_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    
    
    # Add processing timestamp
    df = df.select(*[col(c) for c in final_columns if c in df.columns]) \
           .withColumn("_processed_timestamp", current_timestamp())
    
    return df

