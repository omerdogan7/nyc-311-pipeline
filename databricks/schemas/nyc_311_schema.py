from pyspark.sql.types import *

nyc_311_schema = StructType([
    # Core Fields
    StructField("unique_key", StringType(), True),
    StructField("created_date", StringType(), True),  # Timestamp'e çevrilecek
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
    StructField("x_coordinate_state_plane", DoubleType(), True),
    StructField("y_coordinate_state_plane", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("location", StringType(), True),  # Point type
    
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