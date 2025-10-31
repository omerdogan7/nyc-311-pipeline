-- 311 Complaints Fact Table (FIXED - Corrected Partitioning)
{{ 
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    partition_by=['created_year'],
    cluster_by=['agency_code','created_month'],
    on_schema_change='fail',
    tags=['fact', 'daily']
  ) 
}}

-- Alternative option if you NEED partitioning:
-- partition_by='date(created_date)', 
-- cluster_by=['borough', 'agency_key']
-- Note: This would require adding a created_date timestamp column

{% if is_incremental() %}
    {% set max_timestamp_query %}
        select coalesce(max(_unified_processed_timestamp), cast('1900-01-01' as timestamp))
        from {{ this }}
    {% endset %}
    {% set max_timestamp = run_query(max_timestamp_query).columns[0][0] %}
{% endif %}

with silver_data as (
    select * from {{ source('silver_311', 'silver_311') }}
    {% if is_incremental() %}
        where _unified_processed_timestamp > '{{ max_timestamp }}'
    {% endif %}
),

-- Join with dim tables to get surrogate keys
fact_prep as (
    select
        -- Primary Key
        s.unique_key,
        
        -- Foreign Keys (using surrogate keys from dimensions)
        a.agency_id as agency_key,
        l.location_id as location_key,
        c.complaint_type_key,
        
        -- Denormalized attributes for semantic layer & direct querying
        upper(coalesce(s.borough, 'UNSPECIFIED')) as borough,
        upper(coalesce(s.city, 'UNKNOWN')) as city,
        upper(coalesce(s.agency, 'UNKNOWN')) as agency_code,
        upper(coalesce(s.complaint_type, 'UNKNOWN')) as complaint_type,
        upper(coalesce(s.descriptor, 'NO DESCRIPTOR')) as descriptor,

        -- 👇 NEW: Date derived columns for partitioning & clustering
        year(date(s.created_date)) as created_year,
        month(date(s.created_date)) as created_month,
        
        -- Date Keys (integer for fast joins)
        cast(date_format(date(s.created_date), 'yyyyMMdd') as int) as created_date_key,
        cast(date_format(date(s.closed_date), 'yyyyMMdd') as int) as closed_date_key,
        
        -- Time Attributes (from silver)
        hour(s.created_date) as created_hour,
        case 
            when hour(s.created_date) between 9 and 17 
                and dayofweek(s.created_date) between 2 and 6 
            then true 
            else false 
        end as is_business_hours,
        
        -- Response Time Measures (using data quality flags)
        case 
            when s.is_closed 
                and s.closed_date is not null 
                and not s.has_invalid_closed_date
                and not s.has_historical_closed_date
                and not s.has_future_closed_date
                and s.closed_date >= s.created_date
            then round(
                cast((unix_timestamp(s.closed_date) - unix_timestamp(s.created_date)) as double) / 3600,
                2
            )
            else null 
        end as response_time_hours,
        
        case 
            when s.is_closed 
                and s.closed_date is not null 
                and not s.has_invalid_closed_date
                and not s.has_historical_closed_date
                and not s.has_future_closed_date
                and s.closed_date >= s.created_date
            then round(
                cast((unix_timestamp(s.closed_date) - unix_timestamp(s.created_date)) as double) / 86400,
                2
            )
            else null 
        end as response_time_days,
        
        case 
            when not s.is_closed then 'OPEN'
            when s.closed_date is null then 'NO_DATE'
            when s.has_invalid_closed_date or s.has_historical_closed_date or s.has_future_closed_date then 'INVALID_DATE'
            when s.closed_date < s.created_date then 'NEGATIVE_TIME'
            when datediff(s.closed_date, s.created_date) = 0 then 'SAME_DAY'
            when datediff(s.closed_date, s.created_date) <= 7 then 'WITHIN_WEEK'
            when datediff(s.closed_date, s.created_date) <= 30 then 'WITHIN_MONTH'
            else 'OVER_MONTH'
        end as response_sla_category,
        
        -- Status & Resolution (from silver)
        s.status,
        s.is_closed,
        s.has_resolution,
        s.resolution_description,
        s.resolution_action_updated_date,
        
        -- Location Details (from silver)
        s.latitude,
        s.longitude,
        s.location_type,
        s.has_valid_location,
        s.incident_zip,
        s.incident_address,
        
        -- Additional Location Context
        s.community_board,
        s.bbl,
        s.x_coordinate_state_plane,
        s.y_coordinate_state_plane,
        
        -- Closed Date Audit (from silver)
        s.closed_date_source,
        s.has_invalid_closed_date,
        s.has_historical_closed_date,
        s.has_future_closed_date,
        s.closed_date_original,
        
        -- Data Quality Flags (from silver)
        s.has_valid_zip,
        s.has_closed_status_without_date,
        s.open_status_with_closed_date,
        
        -- Source Tracking (from silver)
        s.open_data_channel_type,
        s._source_file,
        s._ingested_at,
        
        -- Processing Metadata
        s._unified_processed_timestamp,
        current_timestamp() as loaded_at
        
    from silver_data s
    
    -- Join with dim_agency (using agency_key = agency code)
    left join {{ ref('dim_agency') }} a
        on upper(trim(s.agency)) = a.agency_key
    
    -- Join with dim_location (borough + zip only)
    left join {{ ref('dim_location') }} l
        on upper(coalesce(s.borough, 'UNSPECIFIED')) = l.borough
        and upper(coalesce(s.incident_zip, 'NO_ZIP')) = l.zip_code
    
    -- Join with dim_complaint_type (using hash key)
    left join {{ ref('dim_complaint_type') }} c
        on sha2(concat(
            upper(coalesce(s.complaint_type, 'UNSPECIFIED')), 
            '|', 
            upper(coalesce(s.descriptor, 'NO DESCRIPTOR'))
        ), 256) = c.complaint_type_key
),

-- Enrich with date attributes
fact_with_date_attrs as (
    select 
        f.*,
        -- Date attributes from dim_date
        coalesce(d.is_weekend, false) as is_created_on_weekend,
        coalesce(d.is_holiday, false) as is_created_on_holiday,
        coalesce(d.is_business_day, true) as is_created_on_business_day,
        d.fiscal_year as created_fiscal_year
    from fact_prep f
    left join {{ ref('dim_date') }} d
        on f.created_date_key = d.date_key
),

-- Final fact table
final as (
    select
        -- Keys
        unique_key,
        agency_key,
        location_key,
        complaint_type_key,
        created_date_key,
        closed_date_key,
        
        -- Denormalized dimensions (for semantic layer without joins)
        borough,
        city,
        agency_code,
        complaint_type,
        descriptor,

        -- Date Derived Columns (for partition/cluster)
        created_year,
        created_month,
        
        -- Time Attributes
        created_hour,
        is_business_hours,
        is_created_on_weekend,
        is_created_on_holiday,
        is_created_on_business_day,
        created_fiscal_year,
       
        -- Measures (aggregatable metrics - only valid ones)
        response_time_hours,
        response_time_days,
        response_sla_category,
        
        -- Status & Resolution
        status,
        is_closed,
        has_resolution,
        resolution_description,
        resolution_action_updated_date,
        
        -- Location Details
        latitude,
        longitude,
        location_type,
        has_valid_location,
        incident_zip,
        incident_address,
        community_board,
        bbl,
        x_coordinate_state_plane,
        y_coordinate_state_plane,
        
        -- Closed Date Audit
        closed_date_source,
        has_invalid_closed_date,
        has_historical_closed_date,
        has_future_closed_date,
        closed_date_original,
        
        -- Data Quality Flags
        has_valid_zip,
        has_closed_status_without_date,
        open_status_with_closed_date,
        
        -- Source Tracking
        open_data_channel_type,
        _source_file,
        _ingested_at,
        
        -- Metadata
        _unified_processed_timestamp,
        loaded_at
        
    from fact_with_date_attrs
)

select * from final

-- ========================================
-- POST-PROCESSING VALIDATION QUERIES
-- ========================================
/*
After initial load, run these validation queries:

-- 1. Check NULL foreign keys (should be 0 or minimal)
SELECT 
  'agency_key' as fk_name,
  COUNT(*) as null_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ this }}), 4) as null_pct
FROM {{ this }}
WHERE agency_key IS NULL
UNION ALL
SELECT 
  'location_key',
  COUNT(*),
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ this }}), 4)
FROM {{ this }}
WHERE location_key IS NULL
UNION ALL
SELECT 
  'complaint_type_key',
  COUNT(*),
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ this }}), 4)
FROM {{ this }}
WHERE complaint_type_key IS NULL;

-- 2. Check date key validity
SELECT 
  COUNT(*) as invalid_date_keys
FROM {{ this }}
WHERE created_date_key < 20100101 OR created_date_key > 20301231;

-- 3. Borough distribution validation
SELECT 
  borough,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
FROM {{ this }}
GROUP BY borough
ORDER BY count DESC;

-- 4. Incremental load validation (run after 2nd load)
SELECT 
  date(loaded_at) as load_date,
  COUNT(*) as records_loaded,
  MIN(_unified_processed_timestamp) as earliest_record,
  MAX(_unified_processed_timestamp) as latest_record
FROM {{ this }}
GROUP BY date(loaded_at)
ORDER BY load_date DESC;

-- 5. Response time metrics validation
SELECT 
  agency_code,
  COUNT(*) as total_closed,
  ROUND(AVG(response_time_hours), 2) as avg_hours,
  ROUND(AVG(response_time_days), 2) as avg_days,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time_days) as median_days
FROM {{ this }}
WHERE is_closed = true
GROUP BY agency_code
ORDER BY total_closed DESC
LIMIT 10;

-- 6. Data quality summary
SELECT 
  'Total Records' as metric,
  COUNT(*) as value
FROM {{ this }}
UNION ALL
SELECT 'Has Location Key', COUNT(*) FROM {{ this }} WHERE location_key IS NOT NULL
UNION ALL
SELECT 'Has Agency Key', COUNT(*) FROM {{ this }} WHERE agency_key IS NOT NULL
UNION ALL
SELECT 'Has Complaint Key', COUNT(*) FROM {{ this }} WHERE complaint_type_key IS NOT NULL
UNION ALL
SELECT 'Has Valid Coordinates', COUNT(*) FROM {{ this }} WHERE has_valid_location = true
UNION ALL
SELECT 'Closed Cases', COUNT(*) FROM {{ this }} WHERE is_closed = true
UNION ALL
SELECT 'Has Resolution', COUNT(*) FROM {{ this }} WHERE has_resolution = true;
*/