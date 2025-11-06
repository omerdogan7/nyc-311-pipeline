-- Complaint Type Aggregation - FOR DASHBOARD TOP 10
{{ config(
    materialized='incremental',
    unique_key=['date_key', 'complaint_type_key'],
    incremental_strategy='merge',
    on_schema_change='fail',
    tags=['aggregation', 'daily', 'dashboard']
) }}

{% if is_incremental() %}
    {% set lookback_days = var('incremental_lookback_days', 7) %}
    
    {% set min_date_key_query %}
        select cast(
            date_format(
                date_sub(current_date(), {{ lookback_days }}), 
                'yyyyMMdd'
            ) as int
        )
    {% endset %}
    {% set min_date_key = run_query(min_date_key_query).columns[0][0] %}
{% endif %}

WITH base_data AS (
    SELECT
        f.created_date_key as date_key,
        d.date_actual,
        f.complaint_type_key,
        f.complaint_type,
        f.descriptor,
        ct.complaint_category,
        f.agency_code,
        f.borough,
        f.is_closed,
        
        -- Valid response time
        CASE 
            WHEN f.is_closed 
                AND NOT f.has_invalid_closed_date
                AND NOT f.has_historical_closed_date
                AND NOT f.has_future_closed_date
                AND f.response_time_hours IS NOT NULL
                AND f.response_time_hours >= 0
            THEN f.response_time_hours
            ELSE NULL
        END as valid_response_time_hours
        
    FROM {{ ref('fact_311') }} f
    INNER JOIN {{ ref('dim_date') }} d 
        ON f.created_date_key = d.date_key
    LEFT JOIN {{ ref('dim_complaint_type') }} ct 
        ON f.complaint_type_key = ct.complaint_type_key
    
    {% if is_incremental() %}
    WHERE f.created_date_key >= {{ min_date_key }}
    {% endif %}
),

daily_complaint_metrics AS (
    SELECT
        date_key,
        MAX(date_actual) as date_actual,
        complaint_type_key,
        MAX(complaint_type) as complaint_type,
        MAX(descriptor) as descriptor,
        MAX(complaint_category) as complaint_category,
        
        -- Complaint counts
        COUNT(*) as total_complaints,
        SUM(CASE WHEN is_closed THEN 1 ELSE 0 END) as closed_complaints,
        
        -- Response metrics
        AVG(valid_response_time_hours) as avg_response_time_hours,
        PERCENTILE_APPROX(valid_response_time_hours, 0.5) as median_response_time_hours,
        COUNT(valid_response_time_hours) as valid_response_count,
        
        -- Top agency for this complaint type
        MODE(agency_code) as most_common_agency,
        
        -- Top borough for this complaint type
        MODE(borough) as most_common_borough,
        
        current_timestamp() as created_at
        
    FROM base_data
    GROUP BY date_key, complaint_type_key
)

SELECT * FROM daily_complaint_metrics
ORDER BY date_key DESC, total_complaints DESC

