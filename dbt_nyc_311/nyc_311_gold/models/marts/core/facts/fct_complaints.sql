-- 311 Complaints Fact Table
{{ 
  config(
    materialized='incremental',
    unique_key='unique_key',
    on_schema_change='fail',
    tags=['fact', 'daily']
  ) 
}}

with silver_data as (
    select * from {{ source('silver_311', 'silver_enriched_311') }}
    {% if is_incremental() %}
        where _processed_timestamp > (select max(_processed_timestamp) from {{ this }})
    {% endif %}
),

-- Join with dimensions to get keys
fact_prep as (
    select
        -- Primary Key
        s.unique_key,
        
        -- Foreign Keys to Dimensions
        upper(s.agency) as agency_key,
        
        upper(concat(
            coalesce(s.borough, 'UNSPECIFIED'), '|',
            coalesce(s.city, 'UNKNOWN'), '|',
            coalesce(s.incident_zip, 'NO_ZIP')
        )) as location_key,
        
        sha2(concat(
            coalesce(s.complaint_type, 'UNSPECIFIED'), '|',
            coalesce(s.descriptor, 'NO DESCRIPTOR')
        ), 256) as complaint_type_key,
        
        -- Date Keys
        cast(date_format(date(s.created_date), 'yyyyMMdd') as int) as created_date_key,
        cast(date_format(date(s.closed_date), 'yyyyMMdd') as int) as closed_date_key,
        
        -- Time Attributes
        s.created_hour,
        s.is_business_hours,
        
        -- Measures
        s.response_time_hours,
        s.response_time_days,
        s.response_sla_category,
        
        -- Status & Resolution
        s.status,
        s.is_closed,
        s.has_resolution,
        s.resolution_description,
        s.detailed_outcome_category,
        s.main_outcome_category,
        s.was_inspected,
        s.violation_issued,
        s.requires_follow_up,
        
        -- Location Details (for mapping/analysis)
        s.latitude,
        s.longitude,
        s.location_type,
        s.has_valid_location,
        
        -- Street Information (degenerate dimensions)
        s.incident_address,
        s.street_name,
        s.cross_street_1,
        s.cross_street_2,
        
        -- Processing Metadata
        s._processed_timestamp,
        current_timestamp() as loaded_at
        
    from silver_data s
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
        
        -- Time Attributes
        created_hour,
        is_business_hours,
        
        -- Measures
        response_time_hours,
        response_time_days,
        response_sla_category,
        
        -- Status & Resolution
        status,
        is_closed,
        has_resolution,
        resolution_description,
        detailed_outcome_category,
        main_outcome_category,
        was_inspected,
        violation_issued,
        requires_follow_up,
        
        -- Location Details
        latitude,
        longitude,
        location_type,
        has_valid_location,
        
        -- Metadata
        _processed_timestamp,
        loaded_at
        
    from fact_prep
)

select * from final