-- Volume Forecasting Feature Store for ML Training
{{ 
  config(
    materialized='table',
    tags=['ml_features', 'forecasting']
  ) 
}}

with daily_base as (
    select 
        ads.date_key,
        ads.date_actual,
        ads.total_complaints,
        d.year,
        d.month,
        d.day_of_week_num,
        d.day_name,
        d.is_weekend,
        d.is_holiday,
        d.holiday_name,
        d.is_business_day,
        d.season,
        
        -- COVID period classification
        case 
            when ads.date_actual between '2019-01-01' and '2020-03-15' then 'pre_covid'
            when ads.date_actual between '2020-03-16' and '2021-06-30' then 'covid_peak'
            when ads.date_actual >= '2021-07-01' then 'post_covid'
            else 'other'
        end as covid_period
        
    from {{ ref('agg_daily_summary') }} ads
    inner join {{ ref('dim_date') }} d on ads.date_key = d.date_key
    where ads.date_actual >= '2020-01-01'
),

-- Rolling averages and lag features
time_features as (
    select 
        *,
        
        -- Rolling averages
        avg(total_complaints) over (
            order by date_actual 
            rows between 6 preceding and current row
        ) as rolling_7day_avg,
        
        avg(total_complaints) over (
            order by date_actual 
            rows between 13 preceding and current row
        ) as rolling_14day_avg,
        
        avg(total_complaints) over (
            order by date_actual 
            rows between 29 preceding and current row
        ) as rolling_30day_avg,
        
        -- Lag features
        lag(total_complaints, 1) over (order by date_actual) as lag_1day,
        lag(total_complaints, 7) over (order by date_actual) as lag_7day,
        lag(total_complaints, 365) over (order by date_actual) as lag_365day,
        
        -- Moving standard deviation for volatility
        stddev(total_complaints) over (
            order by date_actual 
            rows between 29 preceding and current row
        ) as rolling_30day_std,
        
        -- Trend calculation (simple linear)
        row_number() over (order by date_actual) as trend_days
        
    from daily_base
),

-- Holiday proximity features
holiday_features as (
    select 
        tf.*,
        
        -- Days to next/previous major holiday
        coalesce(
            datediff(
                (select min(d2.date_actual) 
                 from {{ ref('dim_date') }} d2 
                 where d2.is_holiday 
                   and d2.date_actual > tf.date_actual
                   and d2.holiday_name in ('New Year\'s Day', 'Independence Day', 'Christmas Day')
                ), 
                tf.date_actual
            ), 999
        ) as days_to_next_major_holiday,
        
        coalesce(
            datediff(
                tf.date_actual,
                (select max(d2.date_actual) 
                 from {{ ref('dim_date') }} d2 
                 where d2.is_holiday 
                   and d2.date_actual < tf.date_actual
                   and d2.holiday_name in ('New Year\'s Day', 'Independence Day', 'Christmas Day')
                )
            ), 999
        ) as days_since_last_major_holiday
        
    from time_features tf
),

-- Seasonal and cyclical features
seasonal_features as (
    select 
        *,
        
        -- Sine/Cosine for cyclical patterns
        sin(2 * pi() * day_of_week_num / 7.0) as day_of_week_sin,
        cos(2 * pi() * day_of_week_num / 7.0) as day_of_week_cos,
        
        sin(2 * pi() * month / 12.0) as month_sin,
        cos(2 * pi() * month / 12.0) as month_cos,
        
        -- Quarter indicators
        case 
            when month in (1,2,3) then 'Q1'
            when month in (4,5,6) then 'Q2'
            when month in (7,8,9) then 'Q3'
            when month in (10,11,12) then 'Q4'
        end as quarter,
        
        -- School calendar proxy
        case 
            when month in (7, 8) then true
            when month = 6 and d.day_of_month > 15 then true
            when month = 9 and d.day_of_month < 15 then true
            else false
        end as is_summer_break
        
    from holiday_features
),

-- Weather proxy features (based on historical patterns)
weather_proxy as (
    select 
        *,
        
        -- Temperature proxy (complaints patterns)
        case 
            when month in (12, 1, 2) then 'cold'
            when month in (6, 7, 8) then 'hot'
            when month in (3, 4, 5) then 'mild_spring'
            when month in (9, 10, 11) then 'mild_fall'
        end as temp_season,
        
        -- Heating season flag
        case when month in (10, 11, 12, 1, 2, 3) then true else false end as is_heating_season,
        
        -- Outdoor activity season
        case when month in (4, 5, 6, 7, 8, 9) then true else false end as is_outdoor_season
        
    from seasonal_features
),

final as (
    select
        -- Prophet format columns (ds = date, y = target)
        date_actual as ds,
        total_complaints as y,
        
        -- All features for training
        date_key,
        year,
        month,
        day_of_week_num,
        day_name,
        quarter,
        
        -- Boolean/Categorical features (converted to int for Prophet)
        case when is_weekend then 1 else 0 end as is_weekend,
        case when is_holiday then 1 else 0 end as is_holiday,
        case when is_business_day then 1 else 0 end as is_business_day,
        case when is_summer_break then 1 else 0 end as is_summer_break,
        case when is_heating_season then 1 else 0 end as is_heating_season,
        case when is_outdoor_season then 1 else 0 end as is_outdoor_season,
        
        -- COVID period dummies
        case when covid_period = 'pre_covid' then 1 else 0 end as is_pre_covid,
        case when covid_period = 'covid_peak' then 1 else 0 end as is_covid_peak,
        case when covid_period = 'post_covid' then 1 else 0 end as is_post_covid,
        
        -- Numerical features
        round(rolling_7day_avg, 2) as rolling_7day_avg,
        round(rolling_14day_avg, 2) as rolling_14day_avg,
        round(rolling_30day_avg, 2) as rolling_30day_avg,
        round(rolling_30day_std, 2) as rolling_30day_std,
        
        lag_1day,
        lag_7day,
        lag_365day,
        
        days_to_next_major_holiday,
        days_since_last_major_holiday,
        
        -- Cyclical features
        round(day_of_week_sin, 4) as day_of_week_sin,
        round(day_of_week_cos, 4) as day_of_week_cos,
        round(month_sin, 4) as month_sin,
        round(month_cos, 4) as month_cos,
        
        -- Trend feature
        trend_days,
        
        -- Metadata
        holiday_name,
        covid_period,
        temp_season,
        current_timestamp() as created_at
        
    from weather_proxy
    where total_complaints is not null  -- Remove any null targets
)

select * from final
order by ds