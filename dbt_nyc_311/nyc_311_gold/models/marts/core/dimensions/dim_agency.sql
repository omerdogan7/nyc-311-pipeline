-- Agency Dimension Table
{{ 
  config(
    materialized='table',
    tags=['dimension', 'daily']
  ) 
}}

with distinct_agencies as (
    select distinct
        agency,
        -- Clean up OPERATIONS UNIT prefix for DHS
        case 
            when agency_name like 'OPERATIONS UNIT%' 
            then 'DEPARTMENT OF HOMELESS SERVICES'
            else agency_name
        end as agency_name
    from {{ source('silver_311', 'silver_enriched_311') }}
    where agency is not null
),

final as (
    select 
        -- Natural key as primary key
        upper(trim(agency)) as agency_key,
        
        -- Descriptive Attributes
        trim(agency_name) as agency_name,
        
        -- Detailed Business Categories (9 categories)
        case 
            when upper(agency) in ('NYPD', 'DOB') then 'Public Safety'
            when upper(agency) in ('DSNY', 'DOT', 'DPR') then 'City Services'
            when upper(agency) = 'DEP' then 'Environment & Utilities'
            when upper(agency) in ('DHS', 'DFTA') then 'Social Services'
            when upper(agency) = 'DOE' then 'Education'
            when upper(agency) = 'DOHMH' then 'Health'
            when upper(agency) in ('TLC', 'DCWP') then 'Regulatory & Licensing'
            when upper(agency) in ('3-1-1', 'OTI', 'DOITT', 'EDC', 'OSE') then 'Administration & Technology'
            when upper(agency) = 'HPD' then 'Housing'
            else 'Other'
        end as agency_category,
        
        -- Metadata
        current_timestamp() as created_at
        
    from distinct_agencies
)

select * from final
order by agency_key