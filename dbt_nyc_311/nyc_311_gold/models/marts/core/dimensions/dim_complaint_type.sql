-- Complaint Type Dimension Table
{{ 
  config(
    materialized='table',
    tags=['dimension', 'daily']
  ) 
}}

with distinct_complaint_types as (
    select distinct
        coalesce(complaint_type, 'UNSPECIFIED') as complaint_type,
        coalesce(descriptor, 'NO DESCRIPTOR') as descriptor
    from {{ source('silver_311', 'silver_enriched_311') }}
),

final as (
    select 
        -- Surrogate key using SHA2
        sha2(concat(complaint_type, '|', descriptor), 256) as complaint_type_key,
        
        -- Natural keys
        upper(complaint_type) as complaint_type,
        upper(descriptor) as descriptor,
        
        -- Fixed Categorization - Specific patterns first
        case 
            -- 1. Noise (all types)
            when upper(complaint_type) like '%NOISE%' then 'Noise'
            
            -- 2. Parks & Environment (moved up - before Transportation)
            when upper(complaint_type) like '%TREE%' 
                or upper(complaint_type) like '%PARK%'
                or upper(complaint_type) like '%ANIMAL%'
                or upper(complaint_type) like '%RODENT%'
                or upper(complaint_type) like '%DOG%'
                or upper(complaint_type) like '%AIR%'
                then 'Parks & Environment'
            
            -- 3. Transportation (after animal check)
            when upper(complaint_type) like '%TAXI%'
                or upper(complaint_type) like '%VEHICLE%'
                or upper(complaint_type) like '%FOR HIRE%'
                or upper(complaint_type) like '%FHV%'
                or upper(complaint_type) like '%FERRY%'
                or upper(complaint_type) like '%BUS%'
                or upper(complaint_type) like '%BIKE%'
                or upper(complaint_type) like '%SCOOTER%'
                then 'Transportation'
                
            -- 4. Street & Infrastructure
            when upper(complaint_type) like '%STREET%' 
                or upper(complaint_type) like '%SIDEWALK%'
                or upper(complaint_type) like '%HIGHWAY%'
                or upper(complaint_type) like '%BRIDGE%'
                or upper(complaint_type) like '%TRAFFIC%'
                or upper(complaint_type) like '%SNOW%'
                or upper(complaint_type) like '%LIGHT%'
                or upper(complaint_type) like '%SIGN%'
                or upper(complaint_type) like '%PARKING%'
                or upper(complaint_type) like '%ELECTRIC%'
                or upper(complaint_type) like '%DRIVEWAY%' 
                or upper(complaint_type) in ('OBSTRUCTION', 'BENCH', 'WAYFINDING', 'LINKNYC')
                then 'Street & Infrastructure'
            
            -- 5. Sanitation & Cleanliness
            when upper(complaint_type) like '%GARBAGE%' 
                or upper(complaint_type) like '%SANITATION%'
                or upper(complaint_type) like '%COLLECTION%'
                or upper(complaint_type) like '%RECYCLING%'
                or upper(complaint_type) like '%LITTER%'
                or upper(complaint_type) like '%DIRTY%'
                or upper(complaint_type) like '%UNSANITARY%'
                or upper(complaint_type) like '%DUMPSTER%'
                or upper(complaint_type) like '%DISPOSAL%'
                then 'Sanitation & Cleanliness'
            
            -- 6. Building & Housing
            when upper(complaint_type) like '%BUILDING%' 
                or upper(complaint_type) like '%CONSTRUCTION%'
                or upper(complaint_type) like '%HEAT%'
                or upper(complaint_type) like '%WATER%'
                or upper(complaint_type) like '%PLUMBING%'
                or upper(complaint_type) like '%ELEVATOR%'
                or upper(complaint_type) like '%PAINT%'
                or upper(complaint_type) like '%MOLD%'
                or upper(complaint_type) like '%LEAD%'
                or upper(complaint_type) like '%BOILER%'
                or upper(complaint_type) like '%DOOR%'      -- DOOR/WINDOW
                or upper(complaint_type) like '%WINDOW%'
                then 'Building & Housing'
            
            -- 7. Quality of Life
            when upper(complaint_type) like '%ILLEGAL%'
                or upper(complaint_type) like '%HOMELESS%'
                or upper(complaint_type) like '%GRAFFITI%'
                or upper(complaint_type) like '%VIOLATION%'
                or upper(complaint_type) like '%DRUG%'
                or upper(complaint_type) like '%ENCAMPMENT%'
                or upper(complaint_type) like '%CONSUMER%'  -- CONSUMER COMPLAINT
                or upper(complaint_type) like '%YOUTH%'     -- DISORDERLY YOUTH
                or upper(complaint_type) in ('URINATING IN PUBLIC', 'PANHANDLING', 'SQUEEGEE', 'DRINKING')
                then 'Quality of Life'
            
            -- 8. Health & Safety
            when upper(complaint_type) like '%FOOD%'
                or upper(complaint_type) like '%HEALTH%'
                or upper(complaint_type) like '%SAFETY%'
                or upper(complaint_type) like '%COVID%'
                or upper(complaint_type) like '%ASBESTOS%'
                or upper(complaint_type) like '%SMOKING%'
                or upper(complaint_type) in ('LIFEGUARD', 'WINDOW GUARD', 'TATTOOING', 'TANNING', 
                                              'PET SHOP', 'PET SALE', 'APPLIANCE')
                then 'Health & Safety'
            
            else 'Other'
        end as complaint_category,
        
        -- Metadata
        current_timestamp() as created_at
        
    from distinct_complaint_types
)

select * from final
order by complaint_type, descriptor