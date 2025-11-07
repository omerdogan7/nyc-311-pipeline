-- Slim CI test v1.0

{{ config(
    materialized='table',
    tags=['staging']
) }}

with source as (
    select * from {{ source('silver_311', 'silver_311') }}
),

cleaned as (
    select
        unique_key,
        created_date,
        closed_date,
        complaint_type     
    from source
    where created_date is not null
)

select * from cleaned LIMIT 1000