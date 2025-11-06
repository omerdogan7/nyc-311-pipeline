-- Location Dimension Table (Simplified - No City)
{{ 
  config(
    materialized='table',
    tags=['dimension', 'daily']
  ) 
}}

with distinct_locations as (
    select distinct
        -- Only borough and ZIP for cleaner dimension
        upper(
            case 
                when borough is null or trim(borough) = '' then 'UNSPECIFIED'
                else borough
            end
        ) as borough,
        
        upper(
            case 
                when incident_zip is null or trim(incident_zip) = '' then 'NO_ZIP'
                else incident_zip
            end
        ) as zip_code
        
    from {{ source('silver_311', 'silver_311') }}
),

final as (
    select 
        -- Surrogate key (auto-increment)
        row_number() over (order by borough, zip_code) as location_id,
        
        -- Natural composite key
        concat(borough, '|', zip_code) as location_key,
        
        -- Hash key for fast lookups
        md5(concat(borough, '|', zip_code)) as location_hash,
        
        -- Location attributes
        borough,
        zip_code,
        
        -- Borough categorization
        case 
            when borough in ('MANHATTAN', 'BROOKLYN') then 'Core Boroughs'
            when borough in ('QUEENS', 'BRONX', 'STATEN ISLAND') then 'Outer Boroughs'
            when borough = 'UNSPECIFIED' then 'Unspecified'
            else 'Other'
        end as borough_category,
        
        -- ZIP-based region (derived from ZIP patterns)
        case
            -- Manhattan ZIPs (100xx)
            when zip_code like '100%' or zip_code like '101%' or zip_code like '102%' 
                then 'Manhattan'
            -- Bronx ZIPs (104xx)
            when zip_code like '104%' 
                then 'Bronx'
            -- Staten Island ZIPs (103xx)
            when zip_code like '103%' 
                then 'Staten Island'
            -- Brooklyn ZIPs (112xx)
            when zip_code like '112%' 
                then 'Brooklyn'
            -- Queens ZIPs (111xx, 113xx-116xx)
            when zip_code like '111%' or zip_code like '113%' or 
                 zip_code like '114%' or zip_code like '115%' or zip_code like '116%'
                then 'Queens'
            when zip_code = 'NO_ZIP' then 'Unknown'
            else 'Other'
        end as zip_region,
        
        -- Flags
        case when zip_code != 'NO_ZIP' then true else false end as has_zip,
        case when borough != 'UNSPECIFIED' then true else false end as is_specified,
        
        -- Data quality indicator
        case
            when borough != 'UNSPECIFIED' and zip_code != 'NO_ZIP' then 'Complete'
            when borough != 'UNSPECIFIED' or zip_code != 'NO_ZIP' then 'Partial'
            else 'Incomplete'
        end as data_quality,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from distinct_locations
)

select * from final
order by location_id

-- Expected result: ~1,000-1,500 rows (vs 5,373)
-- Reduction: ~78% fewer rows, faster joins