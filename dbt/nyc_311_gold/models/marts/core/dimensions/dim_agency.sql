-- Agency Dimension Table
{{ 
  config(
    materialized='table',
    tags=['dimension', 'daily']
  ) 
}}

with distinct_agencies as (
    select distinct agency
    from {{ source('silver_311', 'silver_311') }}
    where agency is not null
),

final as (
    select
        row_number() over (order by upper(trim(agency))) as agency_id,
        upper(trim(agency)) as agency_key,

        case
            when upper(agency) = '3-1-1' then '311 Customer Service Center'
            when upper(agency) = 'DCWP' then 'Department of Consumer and Worker Protection'
            when upper(agency) = 'DEP' then 'Department of Environmental Protection'
            when upper(agency) = 'DFTA' then 'Department for the Aging'
            when upper(agency) = 'DHS' then 'Department of Homeless Services'
            when upper(agency) = 'DOB' then 'Department of Buildings'
            when upper(agency) = 'DOE' then 'Department of Education'
            when upper(agency) = 'DOHMH' then 'Department of Health and Mental Hygiene'
            when upper(agency) = 'DOITT' then 'Department of Information Technology and Telecommunications'
            when upper(agency) = 'DOT' then 'Department of Transportation'
            when upper(agency) = 'DPR' then 'Department of Parks and Recreation'
            when upper(agency) = 'DSNY' then 'Department of Sanitation'
            when upper(agency) = 'EDC' then 'Economic Development Corporation'
            when upper(agency) = 'HPD' then 'Housing Preservation and Development'
            when upper(agency) = 'NYPD' then 'New York Police Department'
            when upper(agency) = 'OSE' then 'Office of Special Enforcement'
            when upper(agency) = 'OTI' then 'Office of Technology and Innovation'
            when upper(agency) = 'TLC' then 'Taxi and Limousine Commission'
            when upper(agency) = 'OTHER' then 'Other Agencies'
            else upper(agency)
        end as agency_name,

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

        current_timestamp() as created_at

    from distinct_agencies
)

select * from final
order by agency_key
