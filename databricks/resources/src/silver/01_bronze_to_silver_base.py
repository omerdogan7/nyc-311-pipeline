import dlt
from pyspark.sql.functions import (
    col, trim, upper, to_timestamp, year, month, dayofmonth, hour, dayofweek, 
    when, isnan, isnull, lit, current_timestamp
)

# VIEW YERİNE TABLE KULLAN! ✅
@dlt.table(  
    name="silver_base_311",
    comment="Incremental bronze to silver transformation for 40M records with clustering preparation",
    table_properties={
        "quality": "silver",
        "pipelines.target.schema": "nyc_311_dev.silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_or_drop("valid_unique_key", "unique_key IS NOT NULL")
@dlt.expect_or_drop("valid_created_date", "created_date IS NOT NULL")
def bronze_to_silver_base():
    # Incremental read from bronze table
    df = dlt.read_stream("nyc_311_dev.bronze.nyc_311_raw")
    
    # Columns to drop (>90% null) - 40M kayıt için storage tasarrufu
    columns_to_drop = [
        "_rescued_data", "taxi_company_borough", "road_ramp",
        "bridge_highway_direction", "bridge_highway_name", 
        "bridge_highway_segment", "taxi_pick_up_location",
        "vehicle_type", "facility_type"
    ]
    
    # Check which columns exist before dropping
    existing_columns_to_drop = [col_name for col_name in columns_to_drop if col_name in df.columns]
    if existing_columns_to_drop:
        df = df.drop(*existing_columns_to_drop)
    
    # Cast date columns to timestamp - partition kolonları için kritik
    date_columns = ["created_date", "closed_date", "resolution_action_updated_date"]
    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                when((col(col_name).isNotNull()) & (col(col_name) != ""), 
                     to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
                .otherwise(None)
            )
    
    # Due date handling - priority requests için (incremental processing için null check)
    if "due_date" in df.columns:
        df = df.withColumn(
            "due_date", 
            when((col("due_date").isNotNull()) & (col("due_date") != ""), 
                 to_timestamp(col("due_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
            .otherwise(None)
        )
    
    # Cast coordinate columns to double - incremental processing için hata handling
    coord_columns = ["latitude", "longitude", "x_coordinate_state_plane", "y_coordinate_state_plane"]
    for col_name in coord_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                when((col(col_name).isNotNull()) & (col(col_name) != ""), 
                     col(col_name).cast("double"))
                .otherwise(None)
            )
    
    # Extract from location struct if exists - incremental için safe extraction
    if "location" in df.columns:
        df = df.withColumn("location_latitude", 
                          when(col("location").isNotNull(), 
                               col("location.latitude").cast("double"))
                          .otherwise(None)) \
               .withColumn("location_longitude", 
                          when(col("location").isNotNull(), 
                               col("location.longitude").cast("double"))
                          .otherwise(None)) \
               .drop("location")
    
    # Partition columns oluştur - incremental processing için null-safe
    df = df.withColumn("created_year", 
                      when(col("created_date").isNotNull(), year(col("created_date")))
                      .otherwise(9999)) \
           .withColumn("created_month",
                      when(col("created_date").isNotNull(), month(col("created_date")))
                      .otherwise(99)) \
           .withColumn("created_day",
                      when(col("created_date").isNotNull(), dayofmonth(col("created_date")))
                      .otherwise(99))
    
    # Clustering için ek zaman bilgileri - incremental safe
    df = df.withColumn("created_dayofweek",
                      when(col("created_date").isNotNull(), dayofweek(col("created_date")))
                      .otherwise(0)) \
           .withColumn("created_hour",
                      when(col("created_date").isNotNull(), hour(col("created_date")))
                      .otherwise(99))
    
    # Business hours flag - incremental processing için optimize
    df = df.withColumn("is_business_hours",
                      when((col("created_dayofweek").between(2, 6)) &  # Mon-Fri
                           (col("created_hour").between(8, 17)), True)   # 8AM-5PM
                      .otherwise(False))
    
    # Weekend flag (additional temporal analysis)
    df = df.withColumn("is_weekend",
                      col("created_dayofweek").isin(1, 7))
    
    # Closed date için de year/month - incremental updates için
    df = df.withColumn("closed_year",
                      when(col("closed_date").isNotNull(), year(col("closed_date")))
                      .otherwise(None)) \
           .withColumn("closed_month", 
                      when(col("closed_date").isNotNull(), month(col("closed_date")))
                      .otherwise(None))
    
    # String cleaning - clustering için standardizasyon (incremental safe)
    # Borough standardizasyonu - NYC için özel
    df = df.withColumn("borough",
                      when((col("borough").isNull()) | (col("borough") == "") | 
                           (col("borough") == "Unspecified"), "UNSPECIFIED")
                      .when(upper(trim(col("borough"))).isin([
                          "MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"
                      ]), upper(trim(col("borough"))))
                      .otherwise("OTHER"))
    
    # Agency standardization - clustering için
    df = df.withColumn("agency",
                      when((col("agency").isNull()) | (col("agency") == ""), "UNKNOWN")
                      .otherwise(upper(trim(col("agency")))))
    
    # Complaint type cleaning - incremental processing için robust
    df = df.withColumn("complaint_type",
                      when((col("complaint_type").isNull()) | (col("complaint_type") == ""), "UNKNOWN")
                      .otherwise(upper(trim(col("complaint_type")))))
    
    # Other string columns - incremental için null-safe processing
    string_columns = ["city", "agency_name", "descriptor", "location_type", 
                     "status", "resolution_description", "incident_address"]
    for col_name in string_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, 
                              when((col(col_name).isNull()) | (col(col_name) == ""), None)
                              .otherwise(upper(trim(col(col_name)))))
    
    # Coordinate validation - NYC boundaries (incremental processing için safe)
    df = df.withColumn("latitude",
                      when((col("latitude").isNull()) | (isnan(col("latitude"))) | 
                           (col("latitude") < 40.4) | (col("latitude") > 40.9) |
                           (col("latitude") == 0), None)
                      .otherwise(col("latitude"))) \
           .withColumn("longitude", 
                      when((col("longitude").isNull()) | (isnan(col("longitude"))) |
                           (col("longitude") < -74.3) | (col("longitude") > -73.7) |
                           (col("longitude") == 0), None)
                      .otherwise(col("longitude")))
    
    # Data quality indicators - incremental processing için
    df = df.withColumn("has_valid_location", 
                      (col("latitude").isNotNull()) & (col("longitude").isNotNull())) \
           .withColumn("is_closed", col("closed_date").isNotNull()) \
           .withColumn("has_resolution",
                      (col("resolution_description").isNotNull()) & 
                      (col("resolution_description") != ""))
    
    # Address type classification - incremental processing için
    df = df.withColumn("address_type",
                      when(col("incident_address").isNull(), "NOT_PROVIDED")
                      .when(col("incident_address").contains("INTERSECTION"), "INTERSECTION")
                      .when(col("incident_address").contains("BLOCK"), "BLOCK") 
                      .when(col("incident_address") != "", "ADDRESS")
                      .otherwise("NOT_PROVIDED"))
    
    # Processing metadata - incremental tracking için
    df = df.withColumn("_silver_processed_timestamp", current_timestamp())
    
    return df