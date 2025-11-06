-- tests/assert_closed_after_created.sql
-- Ensures closed complaints have logical date ordering

select 
    unique_key,
    created_date_key,
    closed_date_key
from {{ ref('fact_311') }}
where closed_date_key is not null
  and closed_date_key < created_date_key