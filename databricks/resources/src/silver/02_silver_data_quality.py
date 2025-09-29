import dlt
from pyspark.sql.functions import col, when, isnan, isnull, coalesce, lit, upper, trim, current_timestamp

@dlt.table(  # ← VIEW YERİNE TABLE!
    name="silver_cleaned_311",
    comment="Apply data quality rules with original 269 complaint types for optimal clustering",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_borough", "borough IS NOT NULL")
@dlt.expect("valid_agency", "agency IS NOT NULL") 
@dlt.expect("valid_complaint_type", "complaint_type IS NOT NULL")
def silver_data_quality():
    df = dlt.read_stream("silver_base_311")
    
    # Agency cleaning - 17 standardized agencies
    df = df.withColumn("agency",
                      when((col("agency").isNull()) | (col("agency") == ""), "UNKNOWN")
                      .when(col("agency").isin([
                          "NYPD", "DSNY", "DHS", "DOE", "HPD", "OSE", "DFTA", 
                          "DEP", "EDC", "DOHMH", "DPR", "OTI", "DOT", "TLC", 
                          "DCWP", "DOITT", "DOB", "3-1-1"
                      ]), col("agency"))
                      .otherwise("OTHER"))
    
    # Borough cleaning - 5 boroughs + UNSPECIFIED
    df = df.withColumn("borough",
                      when(col("borough").isNull(), "UNSPECIFIED")
                      .when(col("borough") == "", "UNSPECIFIED") 
                      .when(col("borough") == "Unspecified", "UNSPECIFIED")
                      .when(col("borough").isin([
                          "BRONX", "QUEENS", "MANHATTAN", "BROOKLYN", "STATEN ISLAND"
                      ]), col("borough"))
                      .otherwise("UNSPECIFIED"))
    
    # Complaint type - Keep original 269 types, just handle nulls
    df = df.withColumn("complaint_type",
                      when((col("complaint_type").isNull()) | (col("complaint_type") == ""), "UNKNOWN")
                      .otherwise(col("complaint_type")))
    
    # City normalization - align with borough
    df = df.withColumn("city", 
                      when((col("city").isNull()) | (col("city") == ""), "UNKNOWN")
                      .when(col("borough") == "MANHATTAN", "NEW YORK")
                      .when(col("borough") == "BROOKLYN", "BROOKLYN") 
                      .when(col("borough") == "QUEENS", "QUEENS")
                      .when(col("borough") == "BRONX", "BRONX")
                      .when(col("borough") == "STATEN ISLAND", "STATEN ISLAND")
                      .when(col("borough") == "UNSPECIFIED", "UNKNOWN")
                      .otherwise(upper(trim(col("city")))))
    
    # Address type - keep from base processing
    df = df.withColumn("address_type",
                      when((col("address_type").isNull()) | (col("address_type") == ""), "NOT_PROVIDED")
                      .otherwise(col("address_type")))
    
    # Location type cleaning
    df = df.withColumn("location_type",
                      when((col("location_type").isNull()) | (col("location_type") == ""), "NOT_PROVIDED")
                      .otherwise(col("location_type")))
    
    # Descriptor - keep original values
    df = df.withColumn("descriptor",
                      when((col("descriptor").isNull()) | (col("descriptor") == ""), "NO_DESCRIPTION")
                      .otherwise(col("descriptor")))
    
    # Resolution description
    df = df.withColumn("resolution_description",
                      when((col("resolution_description").isNull()) | 
                           (col("resolution_description") == ""), "NO_RESOLUTION_INFO")
                      .otherwise(col("resolution_description")))
    
    # Status standardization
    if "status" in df.columns:
        df = df.withColumn("status",
                          when((col("status").isNull()) | (col("status") == ""), "UNKNOWN")
                          .otherwise(col("status")))
    
    # NYC coordinate validation - precise NYC boundaries
    df = df.withColumn("latitude",
                      when((col("latitude").isNull()) | (isnan(col("latitude"))) | 
                           (col("latitude") < 40.477399) |   # NYC southern boundary
                           (col("latitude") > 40.917577) |   # NYC northern boundary  
                           (col("latitude") == 0), None)
                      .otherwise(col("latitude"))) \
           .withColumn("longitude", 
                      when((col("longitude").isNull()) | (isnan(col("longitude"))) |
                           (col("longitude") < -74.259090) | # NYC western boundary
                           (col("longitude") > -73.700009) | # NYC eastern boundary
                           (col("longitude") == 0), None)
                      .otherwise(col("longitude")))
    
    # ZIP code validation - NYC ZIP pattern
    df = df.withColumn("incident_zip",
                      when((col("incident_zip").isNull()) | 
                           (col("incident_zip") == "") |
                           (col("incident_zip").rlike("^[0-9]{5}$") == False), None)
                      .otherwise(col("incident_zip")))
    
    # Community Board cleaning (NYC has specific CB numbers)
    if "community_board" in df.columns:
        df = df.withColumn("community_board",
                          when((col("community_board").isNull()) | 
                               (col("community_board") == ""), None)
                          .otherwise(col("community_board")))
    
    # Data quality flags
    df = df.withColumn("has_valid_location", 
                      (col("latitude").isNotNull()) & 
                      (col("longitude").isNotNull())) \
           .withColumn("has_resolution",
                      (col("resolution_description") != "NO_RESOLUTION_INFO")) \
           .withColumn("is_closed", 
                      col("closed_date").isNotNull()) \
           .withColumn("has_valid_zip",
                      col("incident_zip").isNotNull())
    
    # Address components cleaning
    address_fields = ["incident_address", "street_name", "cross_street_1", "cross_street_2", 
                     "intersection_street_1", "intersection_street_2", "landmark"]
    
    for field in address_fields:
        if field in df.columns:
            df = df.withColumn(field,
                              when((col(field).isNull()) | (col(field) == ""), None)
                              .otherwise(upper(trim(col(field)))))
    
    # Remove duplicate columns if exist
    columns_to_remove = ["location", "location_latitude", "location_longitude"]
    existing_columns_to_remove = [c for c in columns_to_remove if c in df.columns]
    if existing_columns_to_remove:
        df = df.drop(*existing_columns_to_remove)
    
    # Processing metadata
    df = df.withColumn("_quality_processed_timestamp", current_timestamp())
    
    return df