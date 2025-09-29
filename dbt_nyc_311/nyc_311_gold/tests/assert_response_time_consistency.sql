-- tests/assert_response_time_consistency.sql
-- Verifies response_time_hours and response_time_days are mathematically consistent

select 
    unique_key,
    response_time_hours,
    response_time_days,
    round(response_time_hours / 24.0, 2) as calculated_days
from {{ ref('fct_complaints') }}
where response_time_hours is not null
  and response_time_days is not null
  and abs(response_time_days - (response_time_hours / 24.0)) > 1