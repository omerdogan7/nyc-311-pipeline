import dlt
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import *

# Schema definition for NYC 311 data
nyc_311_schema = StructType([
    # Core Fields
    StructField("unique_key", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("closed_date", StringType(), True),
    StructField("agency", StringType(), True),
    StructField("agency_name", StringType(), True),
    StructField("complaint_type", StringType(), True),
    StructField("descriptor", StringType(), True),
    
    # Location Fields
    StructField("location_type", StringType(), True),
    StructField("incident_zip", StringType(), True),
    StructField("incident_address", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("cross_street_1", StringType(), True),
    StructField("cross_street_2", StringType(), True),
    StructField("intersection_street_1", StringType(), True),
    StructField("intersection_street_2", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("landmark", StringType(), True),
    
    # Status Fields
    StructField("facility_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("due_date", StringType(), True),
    StructField("resolution_description", StringType(), True),
    StructField("resolution_action_updated_date", StringType(), True),
    
    # Geographic Fields
    StructField("community_board", StringType(), True),
    StructField("bbl", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("x_coordinate_state_plane", StringType(), True),
    StructField("y_coordinate_state_plane", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    
    # Location - Nested Struct
    StructField("location", StructType([
        StructField("human_address", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True)
    ]), True),
    
    # Source & Special Fields
    StructField("open_data_channel_type", StringType(), True),
    StructField("park_facility_name", StringType(), True),
    StructField("park_borough", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("taxi_company_borough", StringType(), True),
    StructField("taxi_pick_up_location", StringType(), True),
    StructField("bridge_highway_name", StringType(), True),
    StructField("bridge_highway_direction", StringType(), True),
    StructField("road_ramp", StringType(), True),
    StructField("bridge_highway_segment", StringType(), True)
])

@dlt.table(
    name="nyc_311_raw",
    comment="Raw NYC 311 data from External Volume - 42 raw columns + 4 metadata columns",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_nyc_311_raw():
    """
    Bronze layer reading from Unity Catalog External Volume
    
    Source: /Volumes/nyc_311_dev/bronze/raw_files/
    → Points to: s3://nyc-311-raw/
    
    Raw Data (41 columns):
    - Schema covers all 42 columns from 2025 data
    - 2010 data (31 columns) will have 11 NULL columns automatically
    - schemaEvolutionMode handles future new columns
    
    Metadata Added (4 columns):
    - _ingested_at: When this record was ingested into Bronze
    - _source_file: Which file this record came from
    - _file_modification_time: When the source file was last modified
    - _file_size: Size of the source file in bytes
    
    NO transformations, NO filtering, NO dropping records.
    All data quality checks happen in Silver layer.
    """
    
    # Read from External Volume (Unity Catalog managed path)
    source_path = "/Volumes/nyc_311_dev/bronze/raw_files/"
    
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/Volumes/nyc_311_dev/bronze/checkpoints/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("pathGlobFilter", "*.parquet")
        .option("recursiveFileLookup", "true")
        .option("cloudFiles.maxFilesPerTrigger", "100")
        .load(source_path)  # ← External Volume path
    )
    
    # Add provenance metadata for lineage and debugging
    return (df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
        .withColumn("_file_size", col("_metadata.file_size"))
    )