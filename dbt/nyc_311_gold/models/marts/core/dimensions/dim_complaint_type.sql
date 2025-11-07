-- Complaint Type Dimension Table - REVISED & OPTIMIZED
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
    from {{ source('silver_311', 'silver_311') }}
),

categorized as (
    select
        sha2(concat(complaint_type, '|', descriptor), 256) as complaint_type_key,
        upper(complaint_type) as complaint_type,
        upper(descriptor) as descriptor,

        case
            -- 🔴 0. DATA QUALITY ISSUES (check first!)
            when upper(complaint_type) like '%SELECT%FROM%'
                or upper(complaint_type) like '%WINDOWS%WIN.INI%'
                or upper(complaint_type) like '%WEB-INF%'
                or upper(complaint_type) like '%NSLOOKUP%'
                or upper(complaint_type) like '%SLEEP(%'
                or upper(complaint_type) like '%XMLTYPE%'
                or upper(complaint_type) like '%EVAL(%'
                or upper(complaint_type) like '%OGNL%'
                or upper(complaint_type) like '%ETC/PASSWD%'
                or upper(complaint_type) like '%ECHO%'
                or upper(complaint_type) like '%DECLARE @%'
                then 'Data Quality Issue'

            -- 🔧 1. INTERNAL/TEST DATA
            when upper(complaint_type) in ('DSNY INTERNAL', 'DPR INTERNAL', 'ZTESTINT',
                                           'SRGOVG', 'INTERNAL CODE', 'ATF', 'ZSYSTEST',
                                           'SG-99', 'INCORRECT DATA', 'NONCONST',
                                           'MISCELLANEOUS CATEGORIES', 'BOROUGH OFFICE',
                                           'SPECIAL PROJECTS INSPECTION TEAM (SPIT)',
                                           'INVESTIGATIONS AND DISCIPLINE (IAD)',
                                           'PUBLIC PAYPHONE COMPLAINT', 'EXECUTIVE INSPECTIONS',
                                           'FORENSIC ENGINEERING', 'AHV INSPECTION UNIT',
                                           'SPECIAL OPERATIONS', 'DISCIPLINE AND SUSPENSION',
                                           'REGISTRATION AND TRANSFERS', 'SPECIAL NATURAL AREA DISTRICT (SNAD)',
                                           'FATF', 'FCST')
                or upper(complaint_type) like '%TEST%'
                then 'Internal/Test Data'

            -- 💰 2. TAX & PROPERTY
            when upper(complaint_type) like '%ADVOCATE%'
                or upper(complaint_type) like '%DOF PROPERTY%'
                or upper(complaint_type) like '%DOF PARKING%'
                or upper(complaint_type) like '%DOR LITERATURE%'
                or upper(complaint_type) in ('SCRIE', 'DRIE', 'TAXPAYER ADVOCATE INQUIRY',
                                              'BENEFIT CARD REPLACEMENT')
                then 'Tax & Property'

            -- 👴 3. SOCIAL SERVICES
            when upper(complaint_type) like '%HOME DELIVERED MEAL%'
                or upper(complaint_type) like '%ELDER%'
                or upper(complaint_type) like '%SENIOR%'
                or upper(complaint_type) like '%ALZHEIMER%'
                or upper(complaint_type) like '%NORC%'
                or upper(complaint_type) like '%HEAP%'
                or upper(complaint_type) like '%DHS ADVANTAGE%'
                or upper(complaint_type) like '%LEGAL SERVICES PROVIDER%'
                or upper(complaint_type) like '%HOME CARE PROVIDER%'
                or upper(complaint_type) like '%CASE MANAGEMENT%'
                or upper(complaint_type) like '%TRANSPORTATION PROVIDER%'
                or upper(complaint_type) like '%BEREAVEMENT%'
                or upper(complaint_type) like '%HOMEBOUND%'
                or upper(complaint_type) in ('HOMELESS PERSON ASSISTANCE', 'HOUSING OPTIONS',
                                              'WEATHERIZATION', 'HOME REPAIR')
                then 'Social Services'

            -- 🔊 4. NOISE (highest volume)
            when upper(complaint_type) like '%NOISE%'
                then 'Noise'

            -- 🏠 5. BUILDING & HOUSING
            when upper(complaint_type) like '%BUILDING%'
                or upper(complaint_type) like '%CONSTRUCTION%'
                or upper(complaint_type) like '%HEAT%'
                or upper(complaint_type) like '%HOT WATER%'
                or upper(complaint_type) like '%PLUMBING%'
                or upper(complaint_type) like '%ELEVATOR%'
                or upper(complaint_type) like '%PAINT%'
                or upper(complaint_type) like '%MOLD%'
                or upper(complaint_type) like '%LEAD%'
                or upper(complaint_type) like '%BOILER%'
                or upper(complaint_type) like '%DOOR/WINDOW%'
                or upper(complaint_type) like '%FLOORING%'
                or upper(complaint_type) like '%FACADES%'
                or upper(complaint_type) like '%SCAFFOLD%'
                or upper(complaint_type) like '%WATER LEAK%'
                or upper(complaint_type) like '%INDOOR SEWAGE%'
                or upper(complaint_type) like '%COOLING TOWER%'
                or upper(complaint_type) in ('APPLIANCE', 'VACANT APARTMENT', 'EVICTION',
                                              'VACANT LOT', 'LOT CONDITION', 'UNSTABLE BUILDING',
                                              'ELECTRIC', 'ELECTRICAL')
                then 'Building & Housing'

            -- 🚧 6. STREET & INFRASTRUCTURE
            when upper(complaint_type) like '%STREET%'
                or upper(complaint_type) like '%SIDEWALK%'
                or upper(complaint_type) like '%HIGHWAY%'
                or upper(complaint_type) like '%BRIDGE%'
                or upper(complaint_type) like '%TUNNEL%'
                or upper(complaint_type) like '%TRAFFIC%'
                or upper(complaint_type) like '%SNOW%'
                or upper(complaint_type) like '%LIGHT%'
                or upper(complaint_type) like '%SIGN%'
                or upper(complaint_type) like '%PARKING%'
                or upper(complaint_type) like '%DRIVEWAY%'
                or upper(complaint_type) like '%CURB%'
                or upper(complaint_type) like '%SEWER%'
                or upper(complaint_type) like '%WATER SYSTEM%'
                or upper(complaint_type) like '%WATER MAINTENANCE%'
                or upper(complaint_type) in ('OBSTRUCTION', 'BENCH', 'WAYFINDING', 'LINKNYC',
                                              'BROKEN MUNI METER', 'BROKEN PARKING METER',
                                              'MAINTENANCE OR FACILITY', 'WATER CONSERVATION',
                                              'STANDING WATER', 'CRANES AND DERRICKS', 'UTILITY PROGRAM',
                                              'PUBLIC TOILET', 'STALLED SITES')
                then 'Street & Infrastructure'

            -- 🗑️ 7. SANITATION & CLEANLINESS
            when upper(complaint_type) like '%GARBAGE%'
                or upper(complaint_type) like '%SANITATION%'
                or upper(complaint_type) like '%COLLECTION%'
                or upper(complaint_type) like '%RECYCLING%'
                or upper(complaint_type) like '%LITTER%'
                or upper(complaint_type) like '%DIRTY%'
                or upper(complaint_type) like '%UNSANITARY%'
                or upper(complaint_type) like '%DUMPSTER%'
                or upper(complaint_type) like '%DISPOSAL%'
                or upper(complaint_type) like '%GRAFFITI%'
                or upper(complaint_type) like '%DUMPING%'
                or upper(complaint_type) like '%SWEEPING%'
                or upper(complaint_type) like '%TRANSFER STATION%'
                or upper(complaint_type) like '%FOAM BAN%'
                or upper(complaint_type) in ('OVERFLOWING LITTER BASKETS', 'ADOPT-A-BASKET',
                                              'ELECTRONICS WASTE', 'DERELICT BICYCLE',
                                              'ELECTRONICS WASTE APPOINTMENT', 'INDUSTRIAL WASTE')
                then 'Sanitation & Cleanliness'

            -- 🌳 8. PARKS & ENVIRONMENT
            when upper(complaint_type) like '%TREE%'
                or upper(complaint_type) like '%PARK%'
                or upper(complaint_type) like '%RODENT%'
                or upper(complaint_type) like '%AIR QUALITY%'
                or upper(complaint_type) like '%WATER QUALITY%'
                or upper(complaint_type) like '%PLANT%'
                or upper(complaint_type) like '%MOSQUITOES%'
                or upper(complaint_type) like '%POISON IVY%'
                or upper(complaint_type) like '%HARBORING BEES%'
                or (upper(complaint_type) like '%ANIMAL%'
                    and upper(complaint_type) not like '%VEHICLE%')
                or upper(complaint_type) like '%DOG%'
                or upper(complaint_type) like '%WILDLIFE%'
                or upper(complaint_type) in ('BEACH/POOL/SAUNA COMPLAINT', 'LIFEGUARD',
                                              'VIOLATION OF PARK RULES', 'OUTDOOR DINING',
                                              'STORM', 'UPROOTED STUMP', 'WOOD PILE REMAINING')
                then 'Parks & Environment'

            -- 🚕 9. TRANSPORTATION
            when upper(complaint_type) like '%TAXI%'
                or upper(complaint_type) like '%VEHICLE%'
                or upper(complaint_type) like '%FOR HIRE%'
                or upper(complaint_type) like '%FHV%'
                or upper(complaint_type) like '%FERRY%'
                or upper(complaint_type) like '%BUS%'
                or upper(complaint_type) like '%BIKE%'
                or upper(complaint_type) like '%SCOOTER%'
                or upper(complaint_type) like '%E-SCOOTER%'
                or upper(complaint_type) like '%GREEN TAXI%'
                or upper(complaint_type) in ('DERELICT VEHICLES', 'ABANDONED VEHICLE',
                                              'BUS STOP SHELTER COMPLAINT')
                then 'Transportation'

            -- 🚨 10. QUALITY OF LIFE
            when upper(complaint_type) like '%ILLEGAL%'
                or upper(complaint_type) like '%HOMELESS%'
                or upper(complaint_type) like '%VIOLATION%'
                or upper(complaint_type) like '%DRUG%'
                or upper(complaint_type) like '%ENCAMPMENT%'
                or upper(complaint_type) like '%CONSUMER COMPLAINT%'
                or upper(complaint_type) like '%DISORDERLY%'
                or upper(complaint_type) like '%FIREWORKS%'
                or upper(complaint_type) like '%NON-EMERGENCY POLICE%'
                or upper(complaint_type) like '%ENFORCEMENT%'
                or upper(complaint_type) like '%NONCOMPLIANCE%'
                or upper(complaint_type) in ('URINATING IN PUBLIC', 'PANHANDLING',
                                              'SQUEEGEE', 'DRINKING', 'VENDING',
                                              'QUALITY OF LIFE', 'POSTING ADVERTISEMENT',
                                              'VENDOR ENFORCEMENT', 'OTHER ENFORCEMENT',
                                              'REAL TIME ENFORCEMENT', 'SUSTAINABILITY ENFORCEMENT',
                                              'CANNABIS RETAILER', 'LEANING BAR')
                then 'Quality of Life'

            -- ⚕️ 11. HEALTH & SAFETY
            when upper(complaint_type) like '%FOOD%'
                or upper(complaint_type) like '%HEALTH%'
                or upper(complaint_type) like '%SAFETY%'
                or upper(complaint_type) like '%COVID%'
                or upper(complaint_type) like '%ASBESTOS%'
                or upper(complaint_type) like '%SMOKING%'
                or upper(complaint_type) like '%HAZARD%'
                or upper(complaint_type) like '%X-RAY%'
                or upper(complaint_type) like '%RADIOACTIVE%'
                or upper(complaint_type) like '%VACCINE%'
                or upper(complaint_type) like '%FACE COVERING%'
                or upper(complaint_type) in ('LIFEGUARD', 'WINDOW GUARD', 'TATTOOING',
                                              'TANNING', 'PET SHOP', 'PET SALE',
                                              'CALORIE LABELING', 'SUMMER CAMP',
                                              'DAY CARE', 'EMERGENCY RESPONSE TEAM (ERT)',
                                              'DRINKING WATER', 'BOTTLED WATER')
                then 'Health & Safety'

            -- 📞 12. GENERAL INQUIRY/REQUEST
            when upper(complaint_type) like '%REQUEST%'
                or upper(complaint_type) like '%INQUIRY%'
                or upper(complaint_type) like '%LITERATURE REQUEST%'
                or upper(complaint_type) like '%QUESTION%'
                or upper(complaint_type) like '%PROPERTY%'
                or upper(complaint_type) in ('GENERAL', 'COMMENTS', 'FORMS', 'SELECT MESSAGE TYPE...',
                                              'AGENCY', 'OEM LITERATURE REQUEST',
                                              'LOST PROPERTY', 'FOUND PROPERTY', 'MASS GATHERING COMPLAINT')
                then 'General Inquiry'

            -- 🏫 13. EDUCATION
            when upper(complaint_type) like '%SCHOOL%'
                or upper(complaint_type) like '%TEACHING%'
                or upper(complaint_type) like '%NO CHILD LEFT BEHIND%'
                then 'Education'

            else 'Other'
        end as complaint_category,

        -- 🚩 Data Quality Flag
        coalesce(upper(complaint_type) like '%SELECT%FROM%'
                or upper(complaint_type) like '%WINDOWS%WIN.INI%'
                or upper(complaint_type) like '%WEB-INF%'
                or upper(complaint_type) like '%NSLOOKUP%', false) as is_malicious_data,

        -- 🧪 Test Data Flag
        coalesce(upper(complaint_type) in ('DSNY INTERNAL', 'ZTESTINT', 'ZSYSTEST', 'NONCONST')
                or upper(complaint_type) like '%TEST%', false) as is_test_data,

        current_timestamp() as created_at

    from distinct_complaint_types
),

final as (
    select
        complaint_type_key,
        complaint_type,
        descriptor,
        complaint_category,
        is_malicious_data,
        is_test_data,
        created_at
    from categorized
)

select * from final
order by complaint_category, complaint_type
