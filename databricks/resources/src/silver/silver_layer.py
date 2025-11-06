import dlt
from pyspark.sql.functions import (
    col, trim, upper, to_timestamp, year, month, dayofmonth, 
    when, isnan, lit, current_timestamp, coalesce, greatest,
    count, sum as spark_sum, round as spark_round, avg, max as spark_max, 
    min as spark_min, date_add, current_date
)
from functools import reduce

# ========================================
# VIEW: Staging for Apply Changes
# ========================================
@dlt.view(
    name="silver_311_staging",
    comment="Staging view for NYC 311 - transformation before SCD merge"
)
def silver_311_staging():
    """
    Reads from bronze and applies all transformations.
    Prepares data for SCD Type 1 apply_changes.
    """
    # ✅ SINGLE SOURCE READ
    df = dlt.read_stream("bronze.nyc_311_raw")
    
    # ========================================
    # SECTION 1: BASE TRANSFORMATIONS 
    # ========================================
    
    # Drop high-null columns for storage optimization
    columns_to_drop = [
        "_rescued_data", "taxi_company_borough", "road_ramp",
        "bridge_highway_direction", "bridge_highway_name", 
        "bridge_highway_segment", "taxi_pick_up_location",
        "vehicle_type", "due_date", "facility_type", "_file_modification_time", 
        "_file_size"
    ]
    
    existing_columns_to_drop = [col_name for col_name in columns_to_drop if col_name in df.columns]
    if existing_columns_to_drop:
        df = df.drop(*existing_columns_to_drop)
    
    # Date column casting with error handling
    date_columns = ["created_date", "closed_date", "resolution_action_updated_date"]
    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                when((col(col_name).isNotNull()) & (col(col_name) != ""), 
                     to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
                .otherwise(None)
            )
    
    # ========================================
    # SECTION 1.5: DATA QUALITY VALIDATION
    # ========================================
    
    # Flag 1: Invalid closed dates (closed_date < created_date)
    df = df.withColumn("has_invalid_closed_date",
        when((col("closed_date").isNotNull()) & 
             (col("created_date").isNotNull()) &
             (col("closed_date") < col("created_date")), lit(True))
        .otherwise(lit(False))
    )
    
    # Flag 2: Historical closed dates (< 2010)
    df = df.withColumn("has_historical_closed_date",
        when((col("closed_date").isNotNull()) & 
             (year(col("closed_date")) < 2010), lit(True))
        .otherwise(lit(False))
    )
    
    # Flag 3: Future closed dates (> 30 days from now)
    df = df.withColumn("has_future_closed_date",
        when((col("closed_date").isNotNull()) & 
             (col("closed_date") > date_add(current_date(), 30)), lit(True))
        .otherwise(lit(False))
    )
    
  # ========================================
    # SECTION 2.0: AGGRESSIVE BOROUGH RECOVERY (EXACT ZIP CODES)
    # ========================================
    
    # Step 1: EXACT ZIP → Borough mapping (NO OVERLAPS!)
    # Source: NYC Department of City Planning Official ZIP-Borough mapping
    
    # Define exact ZIP code mappings to prevent overlap issues
    manhattan_zips = [str(z) for z in range(10001, 10283)]
    bronx_zips = [str(z) for z in range(10451, 10476)]
    brooklyn_zips = [str(z) for z in range(11201, 11257) if z not in [11241, 11242, 11243]]
    staten_island_zips = [str(z) for z in range(10301, 10315)]
    
    # Queens specific ZIPs (no ranges to prevent overlap with Brooklyn/Bronx)
    queens_zips = [
        # Astoria, LIC
        "11101", "11102", "11103", "11104", "11105", "11106",
        # Flushing, Bayside
        "11354", "11355", "11356", "11357", "11358", "11359", "11360",
        "11361", "11362", "11363", "11364", "11365", "11366", "11367",
        # Corona, Elmhurst, Rego Park
        "11368", "11369", "11370", "11371", "11372", "11373", "11374", "11375",
        # Woodside, Maspeth, Middle Village (11385 Ridgewood is Queens)
        "11377", "11378", "11379", "11385",
        # Jamaica, JFK
        "11411", "11412", "11413", "11414", "11415", "11416", "11417",
        # Richmond Hill, Ozone Park
        "11418", "11419", "11420", "11421", "11422", "11423", "11426",
        "11427", "11428", "11429", "11430", "11432", "11433", "11434", "11435", "11436",
        # Rockaways
        "11451", "11690", "11691", "11692", "11693", "11694", "11697",
        # Additional Queens ZIPs
        "11004", "11005"
    ]
    
    df = df.withColumn("borough_from_zip",
        when(col("incident_zip").isNotNull(),
            # MANHATTAN
            when(col("incident_zip").isin(manhattan_zips), lit("MANHATTAN"))
            # BRONX
            .when(col("incident_zip").isin(bronx_zips), lit("BRONX"))
            # BROOKLYN
            .when(col("incident_zip").isin(brooklyn_zips), lit("BROOKLYN"))
            # QUEENS (EXACT ZIPs only)
            .when(col("incident_zip").isin(queens_zips), lit("QUEENS"))
            # STATEN ISLAND
            .when(col("incident_zip").isin(staten_island_zips), lit("STATEN ISLAND"))
            .otherwise(None)
        ).otherwise(None)
    )
    
    # Step 2: Expanded neighborhood → Borough mapping (200+ neighborhoods)
    neighborhood_to_borough = {
        # Brooklyn neighborhoods (expanded)
        "BROOKLYN": [
            "WILLIAMSBURG", "PARK SLOPE", "BUSHWICK", "CROWN HEIGHTS",
            "BEDFORD STUYVESANT", "FLATBUSH", "BAY RIDGE", "SUNSET PARK",
            "BENSONHURST", "CONEY ISLAND", "BROWNSVILLE", "CANARSIE",
            "BOROUGH PARK", "SHEEPSHEAD BAY", "BRIGHTON BEACH", "DUMBO",
            "DOWNTOWN BROOKLYN", "FORT GREENE", "CLINTON HILL", "PROSPECT HEIGHTS",
            "CARROLL GARDENS", "COBBLE HILL", "BOERUM HILL", "RED HOOK",
            "GOWANUS", "GREENPOINT", "EAST NEW YORK", "CYPRESS HILLS",
            "FLATLANDS", "GRAVESEND", "MARINE PARK", "MILL BASIN",
            "MIDWOOD", "DYKER HEIGHTS", "BATH BEACH", "BROOKLYN"
        ],
        
        # Queens neighborhoods (expanded)  
        "QUEENS": [
            "ASTORIA", "FLUSHING", "JACKSON HEIGHTS", "LONG ISLAND CITY",
            "FOREST HILLS", "JAMAICA", "RIDGEWOOD", "WOODSIDE", "ELMHURST",
            "CORONA", "KEW GARDENS", "HOWARD BEACH", "BAYSIDE", "WHITESTONE",
            "FRESH MEADOWS", "REGO PARK", "SUNNYSIDE", "MASPETH", "MIDDLE VILLAGE",
            "GLENDALE", "OZONE PARK", "SOUTH OZONE PARK", "RICHMOND HILL",
            "SOUTH RICHMOND HILL", "KEW GARDENS HILLS", "BRIARWOOD", "HOLLIS",
            "QUEENS VILLAGE", "ROSEDALE", "LAURELTON", "SPRINGFIELD GARDENS",
            "ST ALBANS", "CAMBRIA HEIGHTS", "BELLEROSE", "FLORAL PARK",
            "GLEN OAKS", "NEW HYDE PARK", "DOUGLASTON", "LITTLE NECK",
            "AUBURNDALE", "EAST ELMHURST", "COLLEGE POINT", "MALBA",
            "BEECHHURST", "FAR ROCKAWAY", "ARVERNE", "ROCKAWAY BEACH",
            "BELLE HARBOR", "NEPONSIT", "BREEZY POINT", "BROAD CHANNEL", "QUEENS"
        ],
        
        # Manhattan neighborhoods (expanded)
        "MANHATTAN": [
            "HARLEM", "UPPER WEST SIDE", "UPPER EAST SIDE", "MIDTOWN",
            "CHELSEA", "GREENWICH VILLAGE", "SOHO", "TRIBECA", "CHINATOWN",
            "LOWER EAST SIDE", "WASHINGTON HEIGHTS", "INWOOD", "EAST HARLEM",
            "MORNINGSIDE HEIGHTS", "HAMILTON HEIGHTS", "MANHATTANVILLE",
            "WEST VILLAGE", "EAST VILLAGE", "GRAMERCY", "MURRAY HILL",
            "KIPS BAY", "TURTLE BAY", "SUTTON PLACE", "YORKVILLE",
            "LENOX HILL", "CARNEGIE HILL", "UPPER MANHATTAN", "FINANCIAL DISTRICT",
            "BATTERY PARK CITY", "CIVIC CENTER", "TWO BRIDGES", "NOHO",
            "NOLITA", "LITTLE ITALY", "BOWERY", "ALPHABET CITY",
            "STUYVESANT TOWN", "PETER COOPER VILLAGE", "ROOSEVELT ISLAND",
            "MARBLE HILL", "SUGAR HILL", "STRIVERS ROW", "CLINTON",
            "HELLS KITCHEN", "GARMENT DISTRICT", "MIDTOWN EAST", "MIDTOWN WEST",
            "THEATER DISTRICT", "TIMES SQUARE", "KOREATOWN", "NOMAD",
            "FLATIRON", "UNION SQUARE", "MADISON SQUARE", "NEW YORK", "MANHATTAN"
        ],
        
        # Bronx neighborhoods (expanded)
        "BRONX": [
            "FORDHAM", "RIVERDALE", "KINGSBRIDGE", "PELHAM BAY",
            "MORRISANIA", "HUNTS POINT", "SOUNDVIEW", "PARKCHESTER",
            "THROGS NECK", "CO OP CITY", "WAKEFIELD", "WILLIAMSBRIDGE",
            "BAYCHESTER", "EASTCHESTER", "MORRIS PARK", "VAN NEST",
            "BELMONT", "TREMONT", "MOUNT HOPE", "BEDFORD PARK",
            "NORWOOD", "WOODLAWN", "SPUYTEN DUYVIL", "FIELDSTON",
            "JEROME PARK", "CONCOURSE", "HIGHBRIDGE", "MOUNT EDEN",
            "MELROSE", "MOTT HAVEN", "PORT MORRIS", "LONGWOOD",
            "CLASON POINT", "CASTLE HILL", "UNIONPORT", "WESTCHESTER SQUARE",
            "CITY ISLAND", "COUNTRY CLUB", "PELHAM GARDENS", "BRONX"
        ],
        
        # Staten Island neighborhoods
        "STATEN ISLAND": [
            "ST GEORGE", "NEW BRIGHTON", "PORT RICHMOND", "TOTTENVILLE",
            "STAPLETON", "CLIFTON", "ROSEBANK", "SOUTH BEACH",
            "MIDLAND BEACH", "NEW DORP", "OAKWOOD", "GREAT KILLS",
            "ELTINGVILLE", "ANNADALE", "HUGUENOT", "PRINCES BAY",
            "PLEASANT PLAINS", "CHARLESTON", "ROSSVILLE", "WOODROW",
            "RICHMOND VALLEY", "TRAVIS", "CHELSEA", "BLOOMFIELD",
            "BULLS HEAD", "WILLOWBROOK", "TODT HILL", "EMERSON HILL",
            "LIGHTHOUSE HILL", "DONGAN HILLS", "GRANT CITY", "ARROCHAR",
            "STATEN ISLAND"
        ]
    }
    
    # Create neighborhood lookup condition using nested when() calls
    borough_from_city = None
    for borough, neighborhoods in neighborhood_to_borough.items():
        condition = upper(trim(col("city"))).isin(neighborhoods)
        if borough_from_city is None:
            borough_from_city = when(condition, lit(borough))
        else:
            borough_from_city = borough_from_city.when(condition, lit(borough))
    borough_from_city = borough_from_city.otherwise(None)

    df = df.withColumn("borough_from_city", borough_from_city)
    
    # ========================================
    # SECTION 2.1: FINAL BOROUGH ASSIGNMENT (3-Tier Hierarchy)
    # ========================================
    df = df.withColumn("borough",
        # 1️⃣ PRIORITY: Keep original borough if valid 
        when(
            (col("borough").isNotNull()) & 
            (col("borough") != "") & 
            (col("borough") != "Unspecified") &
            (upper(trim(col("borough"))).isin([
                "MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"
            ])),
            upper(trim(col("borough")))
        )
        # 2️⃣ RECOVERY: for only UNSPECIFIED borough from ZIP/City
        .otherwise(
            coalesce(
                col("borough_from_zip"),    # Priority 1: ZIP code (most reliable)
                col("borough_from_city"),   # Priority 2: Neighborhood name
                lit("UNSPECIFIED")          # Last resort
            )
        )
    ).drop("borough_from_city", "borough_from_zip")
    
    # Complaint type - preserve 269 original types
    df = df.withColumn("complaint_type",
                      when((col("complaint_type").isNull()) | (col("complaint_type") == ""), "UNKNOWN")
                      .otherwise(upper(trim(col("complaint_type")))))
    
    # ========================================
    # SECTION 2.2: City alignment - GRANULAR DATA PRESERVATION
    # ========================================
    # Temp column for cleaned city
    df = df.withColumn("city_temp", 
        when((col("city").isNull()) | (col("city") == ""), None)
        .otherwise(upper(trim(col("city"))))
    )

    # Logic:
    # 1. City NULL  → Fill from Borough
    # 2. City exists + Borough UNSPECIFIED → KEEP City (neighborhood info!)
    # 3. City exists + Borough known → KEEP original city
    df = df.withColumn("city",
        when(col("city_temp").isNull(),
             # Scenario 1: City NULL → Fill from Borough
             when(col("borough") == "MANHATTAN", "NEW YORK")
             .when(col("borough") == "BROOKLYN", "BROOKLYN") 
             .when(col("borough") == "QUEENS", "QUEENS")
             .when(col("borough") == "BRONX", "BRONX")
             .when(col("borough") == "STATEN ISLAND", "STATEN ISLAND")
             .otherwise("UNKNOWN"))
        # Scenario 2: City exists + Borough UNSPECIFIED → KEEP City (neighborhood info!)
        .when(col("borough") == "UNSPECIFIED", 
              coalesce(col("city_temp"), lit("UNKNOWN")))
        # Scenario 3: City exists + Borough known → KEEP original city (WILLIAMSBURG, ASTORIA etc.)
        .otherwise(col("city_temp"))
    ).drop("city_temp")
    
    # ========================================
    # 🆕 SECTION 2.3: CLOSED_DATE INFERENCE FROM RESOLUTION_DATE
    # ========================================
    # Fix for DHS: Use resolution_action_updated_date when closed_date is NULL
    # Coverage: 53,679 records (99.88% of DHS closed cases without closed_date)
    # ⚠️ CRITICAL: Only applies to CLOSED status (not OPEN/IN PROGRESS/PENDING)
    
    # Preserve original for audit trail
    df = df.withColumn("closed_date_original", col("closed_date"))
    
    # Inference logic - ONLY for CLOSED status
    df = df.withColumn("closed_date_inferred",
        when(
            # ✅ CRITICAL: Status must be CLOSED (handles variants like CLOSED - DUPLICATE)
            (upper(col("status")).startswith('CLOSED')) & 
            
            # closed_date is NULL
            (col("closed_date").isNull()) &
            
            # resolution_action_updated_date exists
            (col("resolution_action_updated_date").isNotNull()) &
            
            # Temporal validation: resolution between created and today
            (col("resolution_action_updated_date") >= col("created_date")) &
            (col("resolution_action_updated_date") <= current_date()),
            
            # Use resolution_action_updated_date
            col("resolution_action_updated_date")
        )
        # ✅ Otherwise: NULL (for OPEN/IN PROGRESS/PENDING - this is EXPECTED behavior)
    )
    
    # Apply inference (coalesce: original first, then inferred)
    df = df.withColumn("closed_date",
        coalesce(col("closed_date_original"), col("closed_date_inferred"))
    )
    
    # Source tracking for transparency and audit
    df = df.withColumn("closed_date_source",
        when(col("closed_date_original").isNotNull(), lit("ORIGINAL"))
        .when(col("closed_date_inferred").isNotNull(), lit("INFERRED_FROM_RESOLUTION"))
        .otherwise(lit("MISSING"))  # OPEN/IN PROGRESS/PENDING will have MISSING (expected)
    )
    
    # Cleanup intermediate column
    df = df.drop("closed_date_inferred")
    
    # ========================================
    # SECTION 1.6: SEQUENCE DATE CALCULATION (moved after closed_date inference)
    # ========================================
    
    # ✅ Use greatest() to find the truly latest update
    # Now closed_date is enriched, so updated_date will be more accurate
    df = df.withColumn(
        "updated_date",
        greatest(
            col("created_date"),
            coalesce(col("closed_date"), col("created_date")),
            coalesce(col("resolution_action_updated_date"), col("created_date"))
        )
    )
    
    # Coordinate casting and location struct extraction
    coord_columns = ["latitude", "longitude", "x_coordinate_state_plane", "y_coordinate_state_plane"]
    for col_name in coord_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                when((col(col_name).isNotNull()) & (col(col_name) != ""), 
                     col(col_name).cast("double"))
                .otherwise(None)
            )
    
    # Extract location struct if exists
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
    
    # ✅ Temporal partitioning columns (for partitioning strategy)
    # 🔧 FIXED: Changed default from 9999 to 1900 for better partition pruning
    df = df.withColumn("created_year", 
                      when(col("created_date").isNotNull(), year(col("created_date")))
                      .otherwise(1900)) \
           .withColumn("created_month",
                      when(col("created_date").isNotNull(), month(col("created_date")))
                      .otherwise(1)) \
           .withColumn("created_day",
                      when(col("created_date").isNotNull(), dayofmonth(col("created_date")))
                      .otherwise(1))
    
    # ========================================
    # SECTION 2.4: STRING FIELD STANDARDIZATION
    # ========================================
    
    # String field standardization
    # Special handling for resolution_description based on status
    df = df.withColumn("resolution_description",
                      when((col("resolution_description").isNull()) | (col("resolution_description") == ""), 
                           when(col("status").isin(["OPEN", "ASSIGNED", "PENDING", "IN PROGRESS", "STARTED"]), None)
                           .otherwise("NO_RESOLUTION_INFO"))
                      .otherwise(upper(trim(col("resolution_description")))))
    
    # Other string fields with standard defaults
    string_fields = {
        "descriptor": "NO_DESCRIPTION",
        "location_type": "NOT_PROVIDED",
        "status": "UNKNOWN"
    }
    
    for field, default_val in string_fields.items():
        if field in df.columns:
            df = df.withColumn(field,
                              when((col(field).isNull()) | (col(field) == ""), default_val)
                              .otherwise(upper(trim(col(field)))))
    
    # ✅ Address type classification - single when chain (no override)
    df = df.withColumn("address_type",
                      when((col("incident_address").isNull()) | (col("incident_address") == ""), "NOT_PROVIDED")
                      .when(col("incident_address").contains("INTERSECTION"), "INTERSECTION")
                      .when(col("incident_address").contains("BLOCK"), "BLOCK") 
                      .when(col("incident_address") != "", "ADDRESS")
                      .otherwise("NOT_PROVIDED"))
    
    # Other string columns with null-safe processing
    other_string_columns = ["agency_name", "incident_address", "street_name", 
                           "cross_street_1", "cross_street_2", "intersection_street_1", 
                           "intersection_street_2", "landmark"]
    
    for col_name in other_string_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, 
                              when((col(col_name).isNull()) | (col(col_name) == ""), None)
                              .otherwise(upper(trim(col(col_name)))))
    
    # ========================================
    # SECTION 3: COORDINATE & ZIP VALIDATION
    # ========================================
    
    # Precise NYC coordinate boundaries validation
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
    
    # ZIP code validation - 5-digit pattern
    df = df.withColumn("incident_zip",
                      when((col("incident_zip").isNull()) | 
                           (col("incident_zip") == "") |
                           (col("incident_zip").rlike("^[0-9]{5}$") == False), None)
                      .otherwise(col("incident_zip")))
    
    # Community Board cleaning
    if "community_board" in df.columns:
        df = df.withColumn("community_board",
                          when((col("community_board").isNull()) | 
                               (col("community_board") == ""), None)
                          .otherwise(col("community_board")))
    
    # ========================================
    # SECTION 4: DATA QUALITY FLAGS
    # ========================================
    
    df = df.withColumn("has_valid_location", 
                      (col("latitude").isNotNull()) & (col("longitude").isNotNull())) \
           .withColumn("has_resolution",
                      (col("resolution_description").isNotNull()) & 
                      (col("resolution_description") != "NO_RESOLUTION_INFO")) \
           .withColumn("is_closed", 
                      col("closed_date").isNotNull()) \
           .withColumn("has_valid_zip",
                      col("incident_zip").isNotNull()) \
           .withColumn("has_closed_status_without_date",
                    # ✅ FIXED: Recreated AFTER closed_date inference
                    # Now only truly missing closed_dates will be flagged
                    (upper(col("status")).startswith('CLOSED')) & (col("closed_date").isNull())) \
           .withColumn("open_status_with_closed_date",
                    # ✅ NEW FLAG: Data quality check - OPEN cases should NOT have closed_date
                    (col("status").isin(['OPEN', 'IN PROGRESS', 'PENDING', 'ASSIGNED'])) & 
                    (col("closed_date").isNotNull()))
    
    # Remove any duplicate location columns
    columns_to_remove = ["location_latitude", "location_longitude"]
    existing_columns_to_remove = [c for c in columns_to_remove if c in df.columns]
    if existing_columns_to_remove:
        df = df.drop(*existing_columns_to_remove)
    
    # Processing metadata
    df = df.withColumn("_unified_processed_timestamp", current_timestamp())
    
    return df

# ========================================
# APPLY CHANGES: SCD Type 1 Merge
# ========================================
dlt.create_streaming_table(
    name="silver.silver_311",
    comment="NYC 311 Silver table with SCD Type 1 - Enhanced Borough Recovery (200+ Neighborhoods)",
    # ✅ PARTITION STRATEGY
    partition_cols=["created_year", "created_month"],
    
    table_properties={
        "quality": "silver",
        
        # Auto-optimization settings
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        
        # File size optimization (128MB optimal for large tables)
        "delta.targetFileSize": "128MB",
        
        # Retention policies (compliance-friendly)
        "delta.logRetentionDuration": "30 days",
        "delta.deletedFileRetentionDuration": "7 days",
        
        # Change Data Feed for downstream tracking
        "delta.enableChangeDataFeed": "true",
        
        # ✅ INCREMENTAL PROCESSING SETTINGS
        "pipelines.maxFilesPerTrigger": "1000",
        "delta.checkpoint.writeStatsAsJson": "true",
        
        # Data skipping for performance
        "delta.dataSkippingNumIndexedCols": "32",
        
        # Table metadata
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "2"
    }
)

dlt.apply_changes(
    target="silver.silver_311",
    source="silver_311_staging",
    keys=["unique_key"],
    sequence_by="updated_date",
    stored_as_scd_type=1,
    ignore_null_updates=False
)

