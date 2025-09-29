-- Location Dimension Table
{{ 
  config(
    materialized='table',
    tags=['dimension', 'daily']
  ) 
}}

with distinct_locations as (
    select distinct
        -- Handle null values with COALESCE
        coalesce(borough, 'UNSPECIFIED') as borough,
        coalesce(city, 'UNKNOWN') as city,
        coalesce(incident_zip, 'NO_ZIP') as zip_code
    from {{ source('silver_311', 'silver_enriched_311') }}
),

final as (
    select 
        -- Composite natural key
        upper(concat(
            borough, '|',
            city, '|',
            zip_code
        )) as location_key,
        
        -- Location attributes
        upper(borough) as borough,
        upper(city) as city,
        upper(zip_code) as zip_code,
        
        -- Borough categorization
        case 
            when upper(borough) in ('MANHATTAN', 'BROOKLYN') then 'Core Boroughs'
            when upper(borough) in ('QUEENS', 'BRONX', 'STATEN ISLAND') then 'Outer Boroughs'
            when upper(borough) = 'UNSPECIFIED' then 'Unspecified'
            else 'Other'
        end as borough_category,
        
        -- Flags
        case when zip_code = 'NO_ZIP' then false else true end as has_zip,
        case when borough = 'UNSPECIFIED' then false else true end as is_specified,
        
        -- Metadata
        current_timestamp() as created_at
        
    from distinct_locations
)

select * from final
order by borough, city, zip_code