-- Volume Forecasting Feature Store - OPTIMIZED VERSION
{{ 
  config(
    materialized='table',
    tags=['ml_features', 'forecasting', 'time_series', 'prophet']
  ) 
}}

with daily_base as (
    select
        f.created_date_key as date_key,
        d.date_actual,
        
        -- Volume metrics
        count(*) as total_complaints,
        sum(case when f.is_closed = true then 1 else 0 end) as closed_complaints,
        sum(case when upper(f.status) in ('OPEN', 'ASSIGNED', 'STARTED') then 1 else 0 end) as in_progress_count,
        
        -- Basic performance metrics
        avg(case
            when f.is_closed = true
             and f.response_time_hours is not null
             and f.response_time_hours >= 0
            then f.response_time_hours
        end) as avg_response_time_hours,
        
        -- Date dimensions
        d.year,
        d.month,
        d.is_weekend,
        d.is_holiday,
        d.holiday_name,
        d.is_business_day,
        
        -- Season
        case
            when d.month in (12, 1, 2) then 'Winter'
            when d.month in (3, 4, 5) then 'Spring'
            when d.month in (6, 7, 8) then 'Summer'
            when d.month in (9, 10, 11) then 'Fall'
        end as season

    from {{ ref('fact_311') }} f
    inner join {{ ref('dim_date') }} d
        on f.created_date_key = d.date_key
    where d.date_actual >= date('2020-01-01')
    
    group by 
        f.created_date_key,
        d.date_actual,
        d.year,
        d.month,
        d.is_weekend,
        d.is_holiday,
        d.holiday_name,
        d.is_business_day,
        -- Season case statement
        case
            when d.month in (12, 1, 2) then 'Winter'
            when d.month in (3, 4, 5) then 'Spring'
            when d.month in (6, 7, 8) then 'Summer'
            when d.month in (9, 10, 11) then 'Fall'
        end
),

calculated_metrics as (
    select
        *,
        
        -- Key rates (useful for many models)
        case
            when total_complaints > 0
            then round(closed_complaints * 100.0 / total_complaints, 2)
        end as closure_rate,
        
        case
            when closed_complaints > 0
            then round(in_progress_count * 1.0 / closed_complaints, 2)
        end as workload_ratio

    from daily_base
),

time_features as (
    select
        *,
        
        -- ESSENTIAL rolling averages (most models need these)
        avg(total_complaints) over (
            order by date_actual
            rows between 6 preceding and current row
        ) as rolling_7day_avg,
        
        avg(total_complaints) over (
            order by date_actual
            rows between 29 preceding and current row
        ) as rolling_30day_avg,
        
        -- ESSENTIAL lag features
        lag(total_complaints, 1) over (order by date_actual) as lag_1day,
        lag(total_complaints, 7) over (order by date_actual) as lag_7day,
        lag(total_complaints, 30) over (order by date_actual) as lag_30day,
        lag(total_complaints, 365) over (order by date_actual) as lag_365day,
        
        -- Volatility (useful for anomaly detection)
        stddev(total_complaints) over (
            order by date_actual
            rows between 29 preceding and current row
        ) as rolling_30day_std

    from calculated_metrics
),

seasonal_features as (
    select
        *,
        
        -- Common seasonal patterns
        case when month in (7, 8) then 1 else 0 end as is_summer_break,
        case when month in (10, 11, 12, 1, 2, 3) then 1 else 0 end as is_heating_season,
        case when month in (1, 2, 7, 8) then 1 else 0 end as is_extreme_weather_month,
        
        -- Holiday proximity (fix boolean comparison)
        case when is_holiday = true then 1 else 0 end as is_near_holiday

    from time_features
),

final as (
    select
        -- Prophet columns
        date_actual as ds,
        total_complaints as y,
        
        -- DATE FEATURES (useful for many models)
        date_key,
        year,
        month,
        cast(is_weekend as int) as is_weekend,
        cast(is_holiday as int) as is_holiday,
        cast(is_business_day as int) as is_business_day,
        holiday_name,
        season,
        
        -- VOLUME METRICS
        total_complaints,
        closed_complaints,
        in_progress_count,
        
        -- PERFORMANCE METRICS
        round(closure_rate, 2) as closure_rate,
        round(workload_ratio, 2) as workload_ratio,
        round(avg_response_time_hours, 2) as avg_response_time_hours,
        
        -- TIME SERIES FEATURES (essential)
        round(rolling_7day_avg, 2) as rolling_7day_avg,
        round(rolling_30day_avg, 2) as rolling_30day_avg,
        lag_1day,
        lag_7day,
        lag_30day,
        lag_365day,
        round(rolling_30day_std, 2) as rolling_30day_std,
        
        -- SEASONAL FEATURES
        is_summer_break,
        is_heating_season,
        is_extreme_weather_month,
        is_near_holiday,
        
        -- DERIVED METRICS (useful for analysis)
        case
            when lag_7day > 0
            then round((total_complaints - lag_7day) * 100.0 / lag_7day, 2)
        end as week_over_week_change_pct,
        
        case
            when rolling_30day_std > 0
            then round((total_complaints - rolling_30day_avg) / rolling_30day_std, 3)
        end as z_score_30day,
        
        -- Metadata
        current_timestamp() as created_at

    from seasonal_features
    where total_complaints is not null
)

select * from final
order by ds