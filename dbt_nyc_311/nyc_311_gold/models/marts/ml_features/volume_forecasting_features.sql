-- Volume Forecasting Feature Store - FULLY FACT_311 ALIGNED
{{ 
  config(
    materialized='table',
    tags=['ml_features', 'forecasting', 'time_series', 'prophet']
  ) 
}}

-- ✅ FACT TABLE ALIGNED: Using fact_311 with correct field names
-- ✅ SLA FILTERING: Invalid categories excluded (INVALID_DATE, NEGATIVE_TIME, NO_DATE, OPEN)
-- ✅ NEGATIVE TIME CHECK: response_time_hours >= 0 enforced
-- ✅ PROPER DENOMINATORS: Separate counts for volume vs SLA metrics
-- ✅ NO PHANTOM FIELDS: Only using fields that exist in fact_311

with daily_base as (
    select 
        f.created_date_key as date_key,
        d.date_actual,
        
        -- ============================================
        -- ✅ VOLUME METRICS (All complaints - no SLA filter)
        -- ============================================
        count(*) as total_complaints,
        
        -- Closed complaints (for closure rate calculation)
        sum(case when f.is_closed = true then 1 else 0 end) as closed_complaints,
        
        -- Status breakdown (using status field from fact_311)
        sum(case when upper(f.status) = 'CLOSED' then 1 else 0 end) as completed_count,
        sum(case when upper(f.status) in ('OPEN', 'ASSIGNED', 'STARTED') then 1 else 0 end) as in_progress_count,
        sum(case when upper(f.status) = 'PENDING' then 1 else 0 end) as waiting_count,
        
        -- ============================================
        -- ✅ RESPONSE TIME METRICS (Valid times only)
        -- ============================================
        -- Valid response time count (closed + has time + non-negative)
        sum(case 
            when f.is_closed = true 
             and f.response_time_hours is not null 
             and f.response_time_hours >= 0  -- ✅ NEGATIVE CHECK
            then 1 else 0 
        end) as valid_response_count,
        
        -- Average response time (valid times only)
        avg(case 
            when f.is_closed = true 
             and f.response_time_hours is not null 
             and f.response_time_hours >= 0  -- ✅ NEGATIVE CHECK
            then f.response_time_hours 
        end) as avg_response_time_hours,
        
        -- Median response time
        percentile_approx(
            case 
                when f.is_closed = true 
                 and f.response_time_hours is not null 
                 and f.response_time_hours >= 0  -- ✅ NEGATIVE CHECK
                then f.response_time_hours 
            end, 
            0.5
        ) as median_response_time_hours,
        
        -- ============================================
        -- ✅ SLA METRICS (VALID CATEGORIES ONLY)
        -- ============================================
        -- Valid SLA category count (exclude invalid categories)
        sum(case 
            when f.is_closed = true
             and f.response_sla_category not in ('INVALID_DATE', 'NEGATIVE_TIME', 'NO_DATE', 'OPEN')  -- ✅ CORRECT FILTER
            then 1 else 0 
        end) as total_valid_for_sla,
        
        -- Within SLA count (SAME_DAY + WITHIN_WEEK)
        sum(case 
            when f.is_closed = true
             and f.response_sla_category in ('SAME_DAY', 'WITHIN_WEEK')  -- ✅ MANUAL CALCULATION
            then 1 else 0 
        end) as within_sla_count,
        
        -- Same day count (for additional metric)
        sum(case 
            when f.is_closed = true
             and f.response_sla_category = 'SAME_DAY'
            then 1 else 0 
        end) as same_day_count,
        
        -- Within week count
        sum(case 
            when f.is_closed = true
             and f.response_sla_category = 'WITHIN_WEEK'
            then 1 else 0 
        end) as within_week_count,
        
        -- Within month count
        sum(case 
            when f.is_closed = true
             and f.response_sla_category = 'WITHIN_MONTH'
            then 1 else 0 
        end) as within_month_count,
        
        -- Over month count
        sum(case 
            when f.is_closed = true
             and f.response_sla_category = 'OVER_MONTH'
            then 1 else 0 
        end) as over_month_count,
        
        -- ============================================
        -- ✅ DATA QUALITY METRICS
        -- ============================================
        -- Closed without valid time (for data quality tracking)
        sum(case 
            when f.is_closed = true 
             and (f.response_time_hours is null 
                  or f.response_time_hours < 0)  -- ✅ NEGATIVE AS INVALID
            then 1 else 0 
        end) as closed_without_valid_time,
        
        -- Invalid closed dates tracking
        sum(case when f.has_invalid_closed_date = true then 1 else 0 end) as invalid_closed_dates,
        sum(case when f.has_historical_closed_date = true then 1 else 0 end) as historical_closed_dates,
        sum(case when f.has_future_closed_date = true then 1 else 0 end) as future_closed_dates,
        
        -- Date dimensions
        d.year,
        d.month,
        d.day_of_week_num,
        d.day_name,
        d.is_weekend,
        d.is_holiday,
        d.holiday_name,
        d.is_business_day,
        
        -- Season derived from month
        case 
            when d.month in (12, 1, 2) then 'Winter'
            when d.month in (3, 4, 5) then 'Spring'
            when d.month in (6, 7, 8) then 'Summer'
            when d.month in (9, 10, 11) then 'Fall'
        end as season,
        
        -- COVID period classification
        case 
            when d.date_actual < date('2020-03-16') then 'pre_covid'
            when d.date_actual between date('2020-03-16') and date('2021-06-30') then 'covid_peak'
            when d.date_actual > date('2021-06-30') then 'post_covid'
        end as covid_period
        
    from {{ ref('fact_311') }} f
    inner join {{ ref('dim_date') }} d 
        on f.created_date_key = d.date_key
    where d.date_actual >= date('2020-01-01')
    
    group by 
        f.created_date_key,
        d.date_actual,
        d.year,
        d.month,
        d.day_of_week_num,
        d.day_name,
        d.is_weekend,
        d.is_holiday,
        d.holiday_name,
        d.is_business_day
),

-- ============================================
-- ✅ CALCULATED RATES (After aggregation)
-- ============================================
calculated_metrics as (
    select
        *,
        
        -- Closure rate (closed / total)
        case 
            when total_complaints > 0 
            then round(closed_complaints * 100.0 / total_complaints, 2)
            else null
        end as closure_rate,
        
        -- In progress rate (using in_progress_count)
        case 
            when total_complaints > 0 
            then round(in_progress_count * 100.0 / total_complaints, 2)
            else null
        end as in_progress_rate,
        
        -- ✅ SLA compliance rate (using VALID denominator)
        case 
            when total_valid_for_sla > 0 
            then round(within_sla_count * 100.0 / total_valid_for_sla, 2)
            else null
        end as sla_compliance_rate,
        
        -- ✅ Same day rate (additional metric)
        case 
            when total_valid_for_sla > 0 
            then round(same_day_count * 100.0 / total_valid_for_sla, 2)
            else null
        end as same_day_rate,
        
        -- ✅ Within week rate
        case 
            when total_valid_for_sla > 0 
            then round(within_week_count * 100.0 / total_valid_for_sla, 2)
            else null
        end as within_week_rate,
        
        -- ✅ Within month rate
        case 
            when total_valid_for_sla > 0 
            then round(within_month_count * 100.0 / total_valid_for_sla, 2)
            else null
        end as within_month_rate,
        
        -- ✅ Data quality metric (% closed missing valid time)
        case 
            when closed_complaints > 0 
            then round(closed_without_valid_time * 100.0 / closed_complaints, 2)
            else null
        end as pct_closed_missing_valid_time,
        
        -- ✅ Total invalid date percentage
        case 
            when closed_complaints > 0 
            then round((invalid_closed_dates + historical_closed_dates + future_closed_dates) * 100.0 / closed_complaints, 2)
            else null
        end as pct_invalid_closed_dates
        
    from daily_base
),

-- ============================================
-- ROLLING AVERAGES AND LAG FEATURES (VOLUME)
-- ============================================
time_features as (
    select 
        *,
        
        -- Rolling averages for volume (7, 14, 30, 90 days)
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
        
        avg(total_complaints) over (
            order by date_actual 
            rows between 89 preceding and current row
        ) as rolling_90day_avg,
        
        -- ✅ Rolling averages for performance metrics
        avg(closure_rate) over (
            order by date_actual 
            rows between 6 preceding and current row
        ) as rolling_7day_closure_rate,
        
        avg(avg_response_time_hours) over (
            order by date_actual 
            rows between 6 preceding and current row
        ) as rolling_7day_response_time,
        
        avg(sla_compliance_rate) over (
            order by date_actual 
            rows between 6 preceding and current row
        ) as rolling_7day_sla_rate,
        
        avg(same_day_rate) over (
            order by date_actual 
            rows between 6 preceding and current row
        ) as rolling_7day_same_day_rate,
        
        avg(pct_closed_missing_valid_time) over (
            order by date_actual 
            rows between 6 preceding and current row
        ) as rolling_7day_data_quality_issue_pct,
        
        -- Lag features for volume (1 day, 1 week, 2 weeks, 1 month, 1 year)
        lag(total_complaints, 1) over (order by date_actual) as lag_1day,
        lag(total_complaints, 7) over (order by date_actual) as lag_7day,
        lag(total_complaints, 14) over (order by date_actual) as lag_14day,
        lag(total_complaints, 30) over (order by date_actual) as lag_30day,
        lag(total_complaints, 365) over (order by date_actual) as lag_365day,
        
        -- ✅ Lag features for performance metrics
        lag(closure_rate, 7) over (order by date_actual) as lag_7day_closure_rate,
        lag(avg_response_time_hours, 7) over (order by date_actual) as lag_7day_response_time,
        lag(sla_compliance_rate, 7) over (order by date_actual) as lag_7day_sla_rate,
        lag(same_day_rate, 7) over (order by date_actual) as lag_7day_same_day_rate,
        
        -- Moving statistics for volatility
        stddev(total_complaints) over (
            order by date_actual 
            rows between 29 preceding and current row
        ) as rolling_30day_std,
        
        min(total_complaints) over (
            order by date_actual 
            rows between 29 preceding and current row
        ) as rolling_30day_min,
        
        max(total_complaints) over (
            order by date_actual 
            rows between 29 preceding and current row
        ) as rolling_30day_max,
        
        -- Trend calculation (simple linear - days since start)
        row_number() over (order by date_actual) as trend_days
        
    from calculated_metrics
),

-- ============================================
-- HOLIDAY PROXIMITY FEATURES
-- ============================================
holiday_features as (
    select 
        tf.*,
        
        -- Days to next major holiday
        coalesce(
            datediff(
                (select min(d2.date_actual) 
                 from {{ ref('dim_date') }} d2 
                 where d2.is_holiday = true
                   and d2.date_actual > tf.date_actual
                   and d2.holiday_name in ('New Years Day', 'Independence Day', 
                                           'Christmas Day', 'Thanksgiving')
                ), 
                tf.date_actual
            ), 999
        ) as days_to_next_major_holiday,
        
        -- Days since last major holiday
        coalesce(
            datediff(
                tf.date_actual,
                (select max(d2.date_actual) 
                 from {{ ref('dim_date') }} d2 
                 where d2.is_holiday = true
                   and d2.date_actual < tf.date_actual
                   and d2.holiday_name in ('New Years Day', 'Independence Day', 
                                           'Christmas Day', 'Thanksgiving')
                )
            ), 999
        ) as days_since_last_major_holiday,
        
        -- Is within holiday window (3 days before/after)
        case 
            when exists (
                select 1 from {{ ref('dim_date') }} d2 
                where d2.is_holiday = true
                  and abs(datediff(d2.date_actual, tf.date_actual)) <= 3
            ) then 1
            else 0
        end as is_near_holiday,
        
        -- Specific holiday flags (for seasonal patterns)
        case when tf.holiday_name = 'Independence Day' then 1 else 0 end as is_july_4th_period,
        case when tf.holiday_name = 'Christmas Day' then 1 else 0 end as is_christmas_period,
        case when tf.holiday_name = 'Thanksgiving' then 1 else 0 end as is_thanksgiving_period,
        case when tf.holiday_name = 'New Years Day' then 1 else 0 end as is_new_years_period
        
    from time_features tf
),

-- ============================================
-- SEASONAL AND CYCLICAL FEATURES
-- ============================================
seasonal_features as (
    select 
        *,
        
        -- Sine/Cosine for cyclical patterns (better than one-hot encoding)
        sin(2 * pi() * day_of_week_num / 7.0) as day_of_week_sin,
        cos(2 * pi() * day_of_week_num / 7.0) as day_of_week_cos,
        
        sin(2 * pi() * month / 12.0) as month_sin,
        cos(2 * pi() * month / 12.0) as month_cos,
        
        -- Day of year cyclical encoding
        sin(2 * pi() * datediff(date_actual, date(concat(cast(year as string), '-01-01'))) / 365.0) as day_of_year_sin,
        cos(2 * pi() * datediff(date_actual, date(concat(cast(year as string), '-01-01'))) / 365.0) as day_of_year_cos,
        
        -- Quarter indicators
        case 
            when month in (1,2,3) then 'Q1'
            when month in (4,5,6) then 'Q2'
            when month in (7,8,9) then 'Q3'
            when month in (10,11,12) then 'Q4'
        end as quarter,
        
        -- School calendar proxy (impacts complaint patterns)
        case 
            when month in (7, 8) then true  -- Summer break
            when month = 6 then true  -- School ending
            when month = 9 then true  -- School starting
            else false
        end as is_summer_break
        
    from holiday_features
),

-- ============================================
-- WEATHER PROXY FEATURES
-- ============================================
weather_proxy as (
    select 
        *,
        
        -- Temperature season proxy (based on complaint patterns)
        case 
            when month in (12, 1, 2) then 'cold'
            when month in (6, 7, 8) then 'hot'
            when month in (3, 4, 5) then 'mild_spring'
            when month in (9, 10, 11) then 'mild_fall'
        end as temp_season,
        
        -- Heating season flag (Oct-Mar)
        case when month in (10, 11, 12, 1, 2, 3) then true else false end as is_heating_season,
        
        -- Outdoor activity season (Apr-Sep)
        case when month in (4, 5, 6, 7, 8, 9) then true else false end as is_outdoor_season,
        
        -- Extreme weather months (peak complaints)
        case when month in (1, 2, 7, 8) then true else false end as is_extreme_weather_month
        
    from seasonal_features
),

-- ============================================
-- DERIVED FEATURES (rates of change, ratios)
-- ============================================
derived_features as (
    select
        *,
        
        -- Rate of change features (volume)
        case 
            when lag_1day is not null and lag_1day > 0 
            then round((total_complaints - lag_1day) * 100.0 / lag_1day, 2)
            else null
        end as day_over_day_change_pct,
        
        case 
            when lag_7day is not null and lag_7day > 0 
            then round((total_complaints - lag_7day) * 100.0 / lag_7day, 2)
            else null
        end as week_over_week_change_pct,
        
        case 
            when lag_365day is not null and lag_365day > 0 
            then round((total_complaints - lag_365day) * 100.0 / lag_365day, 2)
            else null
        end as year_over_year_change_pct,
        
        -- ✅ Rate of change for performance metrics
        case 
            when lag_7day_closure_rate is not null 
            then round(closure_rate - lag_7day_closure_rate, 2)
            else null
        end as closure_rate_7day_change,
        
        case 
            when lag_7day_response_time is not null and lag_7day_response_time > 0
            then round((avg_response_time_hours - lag_7day_response_time) * 100.0 / lag_7day_response_time, 2)
            else null
        end as response_time_7day_change_pct,
        
        case 
            when lag_7day_sla_rate is not null 
            then round(sla_compliance_rate - lag_7day_sla_rate, 2)
            else null
        end as sla_rate_7day_change,
        
        case 
            when lag_7day_same_day_rate is not null 
            then round(same_day_rate - lag_7day_same_day_rate, 2)
            else null
        end as same_day_rate_7day_change,
        
        -- Ratio features (current vs rolling avg)
        case 
            when rolling_7day_avg > 0 
            then round(total_complaints / rolling_7day_avg, 3)
            else null
        end as complaints_to_7day_avg_ratio,
        
        case 
            when rolling_30day_avg > 0 
            then round(total_complaints / rolling_30day_avg, 3)
            else null
        end as complaints_to_30day_avg_ratio,
        
        -- Volatility score (current deviation from mean)
        case 
            when rolling_30day_std > 0 
            then round((total_complaints - rolling_30day_avg) / rolling_30day_std, 3)
            else null
        end as z_score_30day,
        
        -- Range utilization (where in min-max range)
        case 
            when rolling_30day_max > rolling_30day_min 
            then round((total_complaints - rolling_30day_min) * 100.0 / (rolling_30day_max - rolling_30day_min), 2)
            else null
        end as range_utilization_pct,
        
        -- ✅ Performance stability score
        case 
            when rolling_7day_sla_rate is not null and sla_compliance_rate is not null
            then round(abs(sla_compliance_rate - rolling_7day_sla_rate), 2)
            else null
        end as sla_rate_volatility,
        
        -- ✅ Workload pressure indicator (in progress vs closed)
        case 
            when closed_complaints > 0
            then round(in_progress_count * 1.0 / closed_complaints, 2)
            else null
        end as workload_pressure_ratio
        
    from weather_proxy
),

-- ============================================
-- FINAL FEATURE SET (Prophet-compatible format)
-- ============================================
final as (
    select
        -- ✅ Prophet standard columns
        date_actual as ds,  -- Date column for Prophet
        total_complaints as y,  -- Target variable for Prophet
        
        -- Identifiers
        date_key,
        year,
        month,
        day_of_week_num,
        day_name,
        quarter,
        
        -- ✅ VOLUME METRICS (fact_311 aligned)
        total_complaints,
        closed_complaints,
        completed_count,
        in_progress_count,
        waiting_count,
        
        -- ✅ PERFORMANCE RATES (fact_311 aligned)
        round(closure_rate, 2) as closure_rate,
        round(in_progress_rate, 2) as in_progress_rate,
        
        -- ✅ RESPONSE TIME METRICS (valid times only, negative filtered)
        valid_response_count,
        round(avg_response_time_hours, 2) as avg_response_time_hours,
        round(median_response_time_hours, 2) as median_response_time_hours,
        
        -- ✅ SLA METRICS (Invalid categories excluded)
        within_sla_count,
        same_day_count,
        within_week_count,
        within_month_count,
        over_month_count,
        total_valid_for_sla,
        round(sla_compliance_rate, 2) as sla_compliance_rate,
        round(same_day_rate, 2) as same_day_rate,
        round(within_week_rate, 2) as within_week_rate,
        round(within_month_rate, 2) as within_month_rate,
        
        -- ✅ DATA QUALITY METRICS
        closed_without_valid_time,
        invalid_closed_dates,
        historical_closed_dates,
        future_closed_dates,
        round(pct_closed_missing_valid_time, 2) as pct_closed_missing_valid_time,
        round(pct_invalid_closed_dates, 2) as pct_invalid_closed_dates,
        
        -- ✅ Data quality tier for filtering
        case 
            when pct_closed_missing_valid_time is null then 'UNKNOWN'
            when pct_closed_missing_valid_time <= 20 then 'HIGH'
            when pct_closed_missing_valid_time <= 40 then 'MEDIUM'
            else 'LOW'
        end as data_quality_tier,
        
        -- ✅ SLA data quality tier (separate)
        case 
            when total_valid_for_sla >= closed_complaints * 0.8 then 'HIGH'
            when total_valid_for_sla >= closed_complaints * 0.6 then 'MEDIUM'
            else 'LOW'
        end as sla_quality_tier,
        
        -- Boolean/Categorical features (converted to int for Prophet)
        cast(is_weekend as int) as is_weekend,
        cast(is_holiday as int) as is_holiday,
        cast(is_business_day as int) as is_business_day,
        cast(is_summer_break as int) as is_summer_break,
        cast(is_heating_season as int) as is_heating_season,
        cast(is_outdoor_season as int) as is_outdoor_season,
        cast(is_extreme_weather_month as int) as is_extreme_weather_month,
        is_near_holiday,
        is_july_4th_period,
        is_christmas_period,
        is_thanksgiving_period,
        is_new_years_period,
        
        -- COVID period dummies (for regime changes)
        case when covid_period = 'pre_covid' then 1 else 0 end as is_pre_covid,
        case when covid_period = 'covid_peak' then 1 else 0 end as is_covid_peak,
        case when covid_period = 'post_covid' then 1 else 0 end as is_post_covid,
        
        -- Rolling statistics features (volume)
        round(rolling_7day_avg, 2) as rolling_7day_avg,
        round(rolling_14day_avg, 2) as rolling_14day_avg,
        round(rolling_30day_avg, 2) as rolling_30day_avg,
        round(rolling_90day_avg, 2) as rolling_90day_avg,
        round(rolling_30day_std, 2) as rolling_30day_std,
        round(rolling_30day_min, 2) as rolling_30day_min,
        round(rolling_30day_max, 2) as rolling_30day_max,
        
        -- ✅ Rolling statistics for performance (from fact_311)
        round(rolling_7day_closure_rate, 2) as rolling_7day_closure_rate,
        round(rolling_7day_response_time, 2) as rolling_7day_response_time,
        round(rolling_7day_sla_rate, 2) as rolling_7day_sla_rate,
        round(rolling_7day_same_day_rate, 2) as rolling_7day_same_day_rate,
        round(rolling_7day_data_quality_issue_pct, 2) as rolling_7day_data_quality_issue_pct,
        
        -- Lag features (volume)
        lag_1day,
        lag_7day,
        lag_14day,
        lag_30day,
        lag_365day,
        
        -- ✅ Lag features for performance (from fact_311)
        round(lag_7day_closure_rate, 2) as lag_7day_closure_rate,
        round(lag_7day_response_time, 2) as lag_7day_response_time,
        round(lag_7day_sla_rate, 2) as lag_7day_sla_rate,
        round(lag_7day_same_day_rate, 2) as lag_7day_same_day_rate,
        
        -- Holiday proximity
        days_to_next_major_holiday,
        days_since_last_major_holiday,
        
        -- Cyclical features (encoded as sin/cos)
        round(day_of_week_sin, 4) as day_of_week_sin,
        round(day_of_week_cos, 4) as day_of_week_cos,
        round(month_sin, 4) as month_sin,
        round(month_cos, 4) as month_cos,
        round(day_of_year_sin, 4) as day_of_year_sin,
        round(day_of_year_cos, 4) as day_of_year_cos,
        
        -- Trend feature
        trend_days,
        
        -- Derived features (rates and ratios - volume)
        day_over_day_change_pct,
        week_over_week_change_pct,
        year_over_year_change_pct,
        complaints_to_7day_avg_ratio,
        complaints_to_30day_avg_ratio,
        z_score_30day,
        range_utilization_pct,
        
        -- ✅ Derived features for performance (from fact_311)
        closure_rate_7day_change,
        response_time_7day_change_pct,
        sla_rate_7day_change,
        same_day_rate_7day_change,
        sla_rate_volatility,
        workload_pressure_ratio,
        
        -- Categorical features (for context)
        holiday_name,
        covid_period,
        temp_season,
        season,
        
        -- Metadata
        current_timestamp() as created_at
        
    from derived_features
    where total_complaints is not null  -- Remove null targets
      and date_actual >= date('2020-01-01')  -- Ensure clean date range
)

select * from final
order by ds

-- ========================================
-- ✅ VALIDATION QUERIES
-- ========================================
/*
-- 1. Verify SLA filtering correctness
SELECT 
    ds as date,
    y as total_complaints,
    closed_complaints,
    total_valid_for_sla,
    within_sla_count,
    same_day_count,
    within_week_count,
    sla_compliance_rate,
    same_day_rate,
    sla_quality_tier,
    CASE 
        WHEN total_valid_for_sla < within_sla_count 
        THEN '❌ ERROR: More within_sla than valid'
        WHEN total_valid_for_sla < same_day_count
        THEN '❌ ERROR: More same_day than valid'
        WHEN closed_complaints < total_valid_for_sla
        THEN '❌ ERROR: More valid SLA than closed'
        WHEN within_sla_count < same_day_count
        THEN '❌ ERROR: same_day should be part of within_sla'
        ELSE '✅ OK' 
    END as sla_validation
FROM {{ this }}
WHERE sla_compliance_rate IS NOT NULL
  AND ds >= current_date - interval '30' day
ORDER BY ds DESC;

-- 2. Check negative time filtering
SELECT 
    ds as date,
    closed_complaints,
    valid_response_count,
    closed_without_valid_time,
    avg_response_time_hours,
    median_response_time_hours,
    pct_closed_missing_valid_time,
    data_quality_tier,
    CASE 
        WHEN avg_response_time_hours < 0 
        THEN '❌ ERROR: Negative average'
        WHEN median_response_time_hours < 0
        THEN '❌ ERROR: Negative median'
        WHEN valid_response_count + closed_without_valid_time != closed_complaints
        THEN '⚠️ WARNING: Counts don''t add up'
        ELSE '✅ OK'
    END as time_validation
FROM {{ this }}
WHERE closed_complaints > 0
ORDER BY ds DESC
LIMIT 100;

-- 3. Data quality distribution
SELECT 
    data_quality_tier,
    sla_quality_tier,
    COUNT(*) as days,
    ROUND(AVG(y), 2) as avg_complaints,
    ROUND(AVG(closure_rate), 2) as avg_closure,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla,
    ROUND(AVG(same_day_rate), 2) as avg_same_day,
    ROUND(AVG(pct_closed_missing_valid_time), 2) as avg_missing_time_pct,
    ROUND(AVG(pct_invalid_closed_dates), 2) as avg_invalid_date_pct
FROM {{ this }}
GROUP BY data_quality_tier, sla_quality_tier
ORDER BY data_quality_tier, sla_quality_tier;

-- 4. Prophet feature completeness check
SELECT 
    COUNT(*) as total_days,
    SUM(CASE WHEN y IS NULL THEN 1 ELSE 0 END) as null_targets,
    SUM(CASE WHEN ds IS NULL THEN 1 ELSE 0 END) as null_dates,
    SUM(CASE WHEN closure_rate IS NOT NULL THEN 1 ELSE 0 END) as days_with_closure_rate,
    SUM(CASE WHEN sla_compliance_rate IS NOT NULL THEN 1 ELSE 0 END) as days_with_sla,
    SUM(CASE WHEN data_quality_tier = 'HIGH' THEN 1 ELSE 0 END) as high_quality_days,
    SUM(CASE WHEN sla_quality_tier = 'HIGH' THEN 1 ELSE 0 END) as high_sla_quality_days,
    ROUND(AVG(pct_closed_missing_valid_time), 2) as avg_data_quality_issue_pct,
    MIN(ds) as min_date,
    MAX(ds) as max_date,
    DATEDIFF(MAX(ds), MIN(ds)) + 1 as date_range_days
FROM {{ this }};

-- 5. SLA category breakdown validation
SELECT 
    ds as date,
    closed_complaints,
    total_valid_for_sla,
    same_day_count,
    within_week_count,
    within_month_count,
    over_month_count,
    same_day_count + within_week_count + within_month_count + over_month_count as sum_categories,
    CASE 
        WHEN same_day_count + within_week_count + within_month_count + over_month_count != total_valid_for_sla
        THEN '❌ ERROR: Categories don''t sum to total_valid_for_sla'
        ELSE '✅ OK'
    END as category_validation
FROM {{ this }}
WHERE total_valid_for_sla > 0
ORDER BY ds DESC
LIMIT 50;

-- 6. Weekly pattern analysis (for Prophet seasonality validation)
SELECT 
    day_name,
    day_of_week_num,
    COUNT(*) as days_count,
    ROUND(AVG(y), 2) as avg_complaints,
    ROUND(AVG(closure_rate), 2) as avg_closure_rate,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_rate,
    ROUND(STDDEV(y), 2) as std_complaints
FROM {{ this }}
WHERE data_quality_tier IN ('HIGH', 'MEDIUM')
GROUP BY day_name, day_of_week_num
ORDER BY day_of_week_num;

-- 7. Monthly seasonality check
SELECT 
    month,
    season,
    COUNT(*) as days_count,
    ROUND(AVG(y), 2) as avg_complaints,
    ROUND(AVG(closure_rate), 2) as avg_closure_rate,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_rate,
    ROUND(AVG(same_day_rate), 2) as avg_same_day_rate,
    ROUND(AVG(pct_closed_missing_valid_time), 2) as avg_data_quality_issue
FROM {{ this }}
WHERE data_quality_tier IN ('HIGH', 'MEDIUM')
GROUP BY month, season
ORDER BY month;

-- 8. COVID period comparison
SELECT 
    covid_period,
    COUNT(*) as days,
    ROUND(AVG(y), 2) as avg_complaints,
    ROUND(AVG(closure_rate), 2) as avg_closure_rate,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_rate,
    ROUND(AVG(avg_response_time_hours), 2) as avg_response_hours,
    COUNT(CASE WHEN data_quality_tier = 'HIGH' THEN 1 END) as high_quality_days
FROM {{ this }}
GROUP BY covid_period
ORDER BY 
    CASE covid_period
        WHEN 'pre_covid' THEN 1
        WHEN 'covid_peak' THEN 2
        WHEN 'post_covid' THEN 3
    END;

-- 9. Rolling features validation (check for nulls at start)
SELECT 
    ds,
    y,
    rolling_7day_avg,
    rolling_30day_avg,
    lag_7day,
    lag_365day,
    CASE 
        WHEN rolling_7day_avg IS NULL AND trend_days > 7 THEN '⚠️ WARNING: Null rolling avg'
        WHEN lag_7day IS NULL AND trend_days > 7 THEN '⚠️ WARNING: Null lag'
        ELSE '✅ OK'
    END as rolling_validation
FROM {{ this }}
ORDER BY ds
LIMIT 100;

-- 10. Outlier detection (extreme values)
SELECT 
    ds as date,
    y as complaints,
    z_score_30day,
    rolling_30day_avg,
    rolling_30day_std,
    ABS(z_score_30day) as abs_z_score,
    CASE 
        WHEN ABS(z_score_30day) > 3 THEN 'Extreme Outlier (>3σ)'
        WHEN ABS(z_score_30day) > 2 THEN 'Moderate Outlier (>2σ)'
        ELSE 'Normal'
    END as outlier_classification,
    holiday_name,
    is_weekend
FROM {{ this }}
WHERE z_score_30day IS NOT NULL
  AND ABS(z_score_30day) > 2
ORDER BY ABS(z_score_30day) DESC
LIMIT 50;

-- 11. Workload pressure trend
SELECT 
    DATE_TRUNC('month', ds) as month,
    ROUND(AVG(workload_pressure_ratio), 2) as avg_workload_pressure,
    ROUND(AVG(in_progress_count), 2) as avg_in_progress,
    ROUND(AVG(closed_complaints), 2) as avg_closed,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_rate,
    COUNT(*) as days_in_month
FROM {{ this }}
WHERE workload_pressure_ratio IS NOT NULL
GROUP BY DATE_TRUNC('month', ds)
ORDER BY month DESC;

-- 12. Feature correlation check (high-level)
SELECT 
    'closure_rate vs sla_rate' as metric_pair,
    CORR(closure_rate, sla_compliance_rate) as correlation
FROM {{ this }}
WHERE closure_rate IS NOT NULL AND sla_compliance_rate IS NOT NULL
UNION ALL
SELECT 
    'complaints vs response_time',
    CORR(y, avg_response_time_hours)
FROM {{ this }}
WHERE y IS NOT NULL AND avg_response_time_hours IS NOT NULL
UNION ALL
SELECT 
    'complaints vs workload_pressure',
    CORR(y, workload_pressure_ratio)
FROM {{ this }}
WHERE y IS NOT NULL AND workload_pressure_ratio IS NOT NULL;
*/

-- ========================================
-- 📊 PROPHET USAGE EXAMPLE
-- ========================================
/*
```python
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

# ============================================
# 1. LOAD AND FILTER DATA
# ============================================
df = spark.table('gold.volume_forecasting_features').toPandas()

# Filter high-quality data only
df_clean = df[df['data_quality_tier'].isin(['HIGH', 'MEDIUM'])].copy()
df_clean = df_clean[df_clean['sla_quality_tier'].isin(['HIGH', 'MEDIUM'])].copy()

print(f"Total days: {len(df)}")
print(f"High-quality days: {len(df_clean)}")
print(f"Data quality distribution:\n{df['data_quality_tier'].value_counts()}")
print(f"SLA quality distribution:\n{df['sla_quality_tier'].value_counts()}")

# ============================================
# 2. TRAIN-TEST SPLIT
# ============================================
train_cutoff = df_clean['ds'].max() - pd.Timedelta(days=90)
train = df_clean[df_clean['ds'] <= train_cutoff].copy()
test = df_clean[df_clean['ds'] > train_cutoff].copy()

print(f"\nTraining: {len(train)} days ({train['ds'].min()} to {train['ds'].max()})")
print(f"Testing: {len(test)} days ({test['ds'].min()} to {test['ds'].max()})")

# ============================================
# 3. PROPHET MODEL CONFIGURATION
# ============================================
model = Prophet(
    growth='linear',
    changepoint_prior_scale=0.05,  # Flexibility for trend changes
    seasonality_prior_scale=10,     # Strength of seasonality
    seasonality_mode='multiplicative',  # NYC complaints show multiplicative seasonality
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False
)

# ============================================
# 4. ADD REGRESSORS (Key Features)
# ============================================
# Temporal features
model.add_regressor('is_weekend')
model.add_regressor('is_holiday')
model.add_regressor('is_near_holiday')
model.add_regressor('is_summer_break')

# COVID regime
model.add_regressor('is_covid_peak')
model.add_regressor('is_post_covid')

# Rolling averages (smooth patterns)
model.add_regressor('rolling_7day_avg')
model.add_regressor('rolling_30day_avg')

# Performance indicators
model.add_regressor('closure_rate')
model.add_regressor('sla_compliance_rate')
model.add_regressor('workload_pressure_ratio')

# Weather/seasonal proxies
model.add_regressor('is_heating_season')
model.add_regressor('is_extreme_weather_month')

# Lag features (autoregressive component)
model.add_regressor('lag_7day')
model.add_regressor('lag_365day')

# ============================================
# 5. TRAIN MODEL
# ============================================
regressor_cols = [
    'is_weekend', 'is_holiday', 'is_near_holiday', 'is_summer_break',
    'is_covid_peak', 'is_post_covid',
    'rolling_7day_avg', 'rolling_30day_avg',
    'closure_rate', 'sla_compliance_rate', 'workload_pressure_ratio',
    'is_heating_season', 'is_extreme_weather_month',
    'lag_7day', 'lag_365day'
]

train_prophet = train[['ds', 'y'] + regressor_cols].copy()

print("\nTraining Prophet model...")
model.fit(train_prophet)
print("✅ Model trained successfully!")

# ============================================
# 6. MAKE PREDICTIONS
# ============================================
test_prophet = test[['ds'] + regressor_cols].copy()
forecast = model.predict(test_prophet)

# Merge predictions with actuals
results = test[['ds', 'y']].merge(
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], 
    on='ds'
)

# ============================================
# 7. EVALUATE PERFORMANCE
# ============================================
mae = mean_absolute_error(results['y'], results['yhat'])
mape = np.mean(np.abs((results['y'] - results['yhat']) / results['y'])) * 100
rmse = np.sqrt(mean_squared_error(results['y'], results['yhat']))

print(f"\n📊 MODEL PERFORMANCE:")
print(f"MAE:  {mae:.2f} complaints/day")
print(f"MAPE: {mape:.2f}%")
print(f"RMSE: {rmse:.2f}")

# Check prediction intervals
within_interval = ((results['y'] >= results['yhat_lower']) & 
                   (results['y'] <= results['yhat_upper'])).mean() * 100
print(f"Within 95% interval: {within_interval:.1f}%")

# ============================================
# 8. ANALYZE COMPONENT IMPORTANCE
# ============================================
from prophet.plot import plot_components, plot_yearly, plot_weekly

# Plot components (trend, weekly, yearly, regressors)
fig1 = model.plot_components(forecast)
fig1.savefig('prophet_components.png')

# Feature importance (from regressor coefficients)
regressor_importance = pd.DataFrame({
    'feature': model.extra_regressors.keys(),
    'coefficient': [model.params['beta'][i] for i in range(len(model.extra_regressors))]
}).sort_values('coefficient', ascending=False)

print(f"\n📈 TOP REGRESSOR COEFFICIENTS:")
print(regressor_importance.head(10))

# ============================================
# 9. FUTURE FORECAST (Next 30 days)
# ============================================
future = model.make_future_dataframe(periods=30)

# Need to populate regressor values for future dates
# Option 1: Use last known values
# Option 2: Use seasonal patterns
# Option 3: Use external calendar data

print(f"\n🔮 Ready to forecast next 30 days")
print(f"Note: Need to populate regressor values for future dates")

# ============================================
# 10. SAVE RESULTS
# ============================================
results.to_csv('forecast_validation.csv', index=False)
print("\n✅ Results saved to forecast_validation.csv")
```

ALTERNATIVE: SIMPLER MODEL (Without Regressors)
================================================
```python
# For quick baseline
model_simple = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False
)
model_simple.fit(train[['ds', 'y']])
forecast_simple = model_simple.predict(test[['ds']])

mae_simple = mean_absolute_error(test['y'], forecast_simple['yhat'])
print(f"Simple model MAE: {mae_simple:.2f}")
```

KEY INSIGHTS FOR NYC 311 FORECASTING:
======================================
1. **Weekly Seasonality**: Strong Mon-Fri > Weekend pattern
2. **Yearly Seasonality**: Summer peak (July-Aug), Winter trough (Dec)
3. **COVID Impact**: Significant regime change in 2020-2021
4. **Holiday Effects**: Major drops on holidays, spikes before/after
5. **Weather Proxy**: Heating season vs outdoor season different patterns
6. **Performance Correlation**: High workload pressure → lower SLA compliance
7. **Data Quality**: Always filter by data_quality_tier = 'HIGH' or 'MEDIUM'

RECOMMENDED FEATURES FOR PROPHET:
==================================
Must-have:
- is_weekend, is_holiday
- rolling_7day_avg (smoothing)
- is_covid_peak (regime change)

Nice-to-have:
- closure_rate, sla_compliance_rate (performance indicators)
- workload_pressure_ratio (capacity signal)
- lag_7day, lag_365day (autoregressive)
- is_heating_season, is_summer_break (seasonal context)

Optional:
- Cyclical encodings (day_of_week_sin/cos, month_sin/cos)
- Holiday proximity features
- Rate of change features
*/