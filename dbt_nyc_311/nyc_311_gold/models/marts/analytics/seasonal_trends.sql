-- Seasonal Trends Analysis on NYC 311 Complaints - FULLY FACT-COMPLIANT
{{ 
  config(
    materialized='table',
    tags=['analytics', 'seasonal', 'trends']
  ) 
}}

with seasonal_classification as (
    select
        d.date_key,
        d.date_actual,
        d.year,
        d.month,
        d.month_name,
        d.day_of_year,
        d.week_of_year,
        
        -- Season classification (meteorological seasons)
        case
            when d.month in (12, 1, 2) then 'Winter'
            when d.month in (3, 4, 5) then 'Spring'  
            when d.month in (6, 7, 8) then 'Summer'
            when d.month in (9, 10, 11) then 'Fall'
        end as season,
        
        -- More granular seasonal periods
        case
            when d.month = 12 or d.month <= 2 then 'Deep Winter'
            when d.month = 3 then 'Early Spring'
            when d.month in (4, 5) then 'Late Spring'
            when d.month in (6, 7) then 'Early Summer'
            when d.month = 8 then 'Late Summer'
            when d.month = 9 then 'Early Fall'
            when d.month in (10, 11) then 'Late Fall'
        end as seasonal_period,
        
        -- NYC-specific seasonal characteristics
        case
            when d.month in (12, 1, 2) then 'Heating Season'
            when d.month in (6, 7, 8) then 'Outdoor Activity Season'
            when d.month in (4, 5, 9, 10) then 'Mild Weather Season'
            when d.month in (3, 11) then 'Transition Season'
        end as nyc_seasonal_context,
        
        -- Holiday-heavy periods
        case
            when d.month = 12 then 'Holiday Season'
            when d.month in (6, 7, 8) then 'Summer Break'
            when d.month = 9 then 'Back to School'
            else 'Regular Period'
        end as activity_period
        
    from {{ ref('dim_date') }} d
    where d.year between 2020 and 2023
),

complaint_seasonal_data as (
    select
        f.unique_key,
        f.created_date_key,
        f.closed_date_key,
        f.is_closed,
        
        -- ✅ Valid response time (with all data quality checks + negative check)
        case 
            when f.is_closed 
                and not f.has_invalid_closed_date
                and not f.has_historical_closed_date
                and not f.has_future_closed_date
                and f.response_time_hours is not null
                and f.response_time_hours >= 0  -- ✅ NEW: Exclude negative values
            then f.response_time_hours
            else null
        end as valid_response_time_hours,
        
        case 
            when f.is_closed 
                and not f.has_invalid_closed_date
                and not f.has_historical_closed_date
                and not f.has_future_closed_date
                and f.response_time_days is not null
                and f.response_time_days >= 0  -- ✅ NEW: Exclude negative values
            then f.response_time_days
            else null
        end as valid_response_time_days,
        
        -- ✅✅ NEW: Valid SLA category (exclude invalid categories)
        case
            when f.response_sla_category in ('INVALID_DATE', 'NEGATIVE_TIME', 'NO_DATE', 'OPEN') then null
            else f.response_sla_category
        end as valid_sla_category,
        
        f.borough,
        f.complaint_type,
        f.descriptor,
        
        -- Data quality flags
        f.has_invalid_closed_date,
        f.has_historical_closed_date,
        f.has_future_closed_date,
        
        sc.season,
        sc.seasonal_period,
        sc.nyc_seasonal_context,
        sc.activity_period,
        sc.month,
        sc.month_name,
        sc.year,
        sc.week_of_year,
        sc.date_actual,
        ct.complaint_category
        
    from {{ ref('fact_311') }} f
    inner join seasonal_classification sc 
        on f.created_date_key = sc.date_key
    left join {{ ref('dim_complaint_type') }} ct 
        on f.complaint_type_key = ct.complaint_type_key
),

-- ============================================
-- MONTHLY SEASONAL PATTERNS
-- ============================================
monthly_patterns as (
    select
        month,
        month_name,
        season,
        nyc_seasonal_context,
        
        -- Volume metrics
        count(*) as total_complaints,
        avg(count(*)) over () as annual_avg_complaints,
        count(distinct year) as years_in_data,
        
        -- Closure metrics
        count(case when is_closed = true then 1 end) as closed_complaints,
        round(count(case when is_closed = true then 1 end) * 100.0 / nullif(count(*), 0), 2) as closure_rate,
        
        -- ✅✅ IMPROVED: Data quality tracking with valid SLA category
        count(case when valid_response_time_hours is not null then 1 end) as complaints_with_valid_response_time,
        round(count(case when valid_response_time_hours is not null then 1 end) * 100.0 / 
            nullif(count(case when is_closed = true then 1 end), 0), 2) as response_time_data_quality_pct,
        
        count(case when valid_sla_category is not null then 1 end) as total_valid_for_sla,
        round(count(case when valid_sla_category is not null then 1 end) * 100.0 / 
            nullif(count(case when is_closed = true then 1 end), 0), 2) as sla_data_quality_pct,
        
        -- Complaint category distribution
        count(case when complaint_category = 'Noise' then 1 end) as noise_complaints,
        count(case when complaint_category = 'Building & Housing' then 1 end) as housing_complaints,
        count(case when complaint_category = 'Parks & Environment' then 1 end) as parks_complaints,
        count(case when complaint_category = 'Street & Infrastructure' then 1 end) as street_complaints,
        count(case when complaint_category = 'Transportation' then 1 end) as transportation_complaints,
        count(case when complaint_category = 'Sanitation & Cleanliness' then 1 end) as sanitation_complaints,
        count(case when complaint_category = 'Quality of Life' then 1 end) as quality_of_life_complaints,
        
        -- Weather-related complaint proxies
        count(case when upper(complaint_type) like '%HEAT%' or upper(complaint_type) like '%HOT WATER%' then 1 end) as heating_complaints,
        count(case when upper(complaint_type) like '%SNOW%' or upper(complaint_type) like '%ICE%' then 1 end) as winter_weather_complaints,
        count(case when upper(complaint_type) like '%AIR%' or upper(complaint_type) like '%COOLING%' then 1 end) as cooling_complaints,
        count(case when upper(complaint_type) like '%TREE%' or upper(complaint_type) like '%BRANCH%' then 1 end) as tree_complaints,
        count(case when upper(complaint_type) like '%POTHOLE%' or upper(complaint_type) like '%STREET CONDITION%' then 1 end) as street_condition_complaints,
        
        -- Performance metrics (VALID DATA ONLY)
        avg(valid_response_time_hours) as avg_response_time_hours,
        avg(valid_response_time_days) as avg_response_time_days,
        percentile_approx(valid_response_time_hours, 0.5) as median_response_time_hours,
        percentile_approx(valid_response_time_days, 0.5) as median_response_time_days,
        
        -- ✅✅ CORRECTED: SLA metrics using VALID SLA category ONLY
        round(count(case when valid_sla_category = 'SAME_DAY' then 1 end) * 100.0 / 
            nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as same_day_closure_rate,
        round(count(case when valid_sla_category = 'WITHIN_WEEK' then 1 end) * 100.0 / 
            nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as within_week_rate,
        round(count(case when valid_sla_category = 'WITHIN_MONTH' then 1 end) * 100.0 / 
            nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as within_month_rate,
        round(count(case when valid_sla_category = 'OVER_MONTH' then 1 end) * 100.0 / 
            nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as over_month_rate,
        
        -- Data quality metrics
        count(case when has_invalid_closed_date = true then 1 end) as invalid_closed_dates,
        count(case when has_historical_closed_date = true then 1 end) as historical_closed_dates,
        count(case when has_future_closed_date = true then 1 end) as future_closed_dates
            
    from complaint_seasonal_data
    where complaint_type is not null
      and complaint_type != 'UNKNOWN'
    group by month, month_name, season, nyc_seasonal_context
),

-- ============================================
-- SEASONAL AVERAGES (for comparison)
-- ============================================
seasonal_averages as (
    select
        season,
        avg(total_complaints) as season_avg_complaints,
        avg(noise_complaints) as season_avg_noise,
        avg(housing_complaints) as season_avg_housing,
        avg(heating_complaints) as season_avg_heating,
        avg(cooling_complaints) as season_avg_cooling,
        avg(winter_weather_complaints) as season_avg_winter_weather,
        avg(avg_response_time_hours) as season_avg_response_time_hours,
        avg(avg_response_time_days) as season_avg_response_time_days,
        avg(same_day_closure_rate) as season_avg_same_day_rate,
        avg(within_week_rate) as season_avg_within_week_rate,
        avg(closure_rate) as season_avg_closure_rate,
        avg(response_time_data_quality_pct) as season_avg_response_quality,
        avg(sla_data_quality_pct) as season_avg_sla_quality
    from monthly_patterns
    group by season
),

-- ============================================
-- BOROUGH-LEVEL SEASONAL PATTERNS
-- ============================================
borough_seasonal as (
    select
        borough,
        season,
        count(*) as total_complaints,
        round(count(*) * 100.0 / sum(count(*)) over (partition by season), 2) as pct_of_season_total,
        
        -- Top complaint categories by borough and season
        count(case when complaint_category = 'Noise' then 1 end) as noise_count,
        count(case when complaint_category = 'Building & Housing' then 1 end) as housing_count,
        count(case when is_closed = true then 1 end) as closed_count,
        round(count(case when complaint_category = 'Noise' then 1 end) * 100.0 / nullif(count(*), 0), 2) as noise_pct,
        round(count(case when complaint_category = 'Building & Housing' then 1 end) * 100.0 / nullif(count(*), 0), 2) as housing_pct,
        round(count(case when is_closed = true then 1 end) * 100.0 / nullif(count(*), 0), 2) as closure_rate,
        round(avg(valid_response_time_days), 2) as avg_response_days
        
    from complaint_seasonal_data
    where borough != 'UNSPECIFIED'
    group by borough, season
),

-- ============================================
-- YEAR-OVER-YEAR SEASONAL COMPARISON
-- ============================================
yoy_seasonal as (
    select
        year,
        season,
        count(*) as complaints,
        count(case when is_closed = true then 1 end) as closed_complaints,
        round(avg(valid_response_time_days), 2) as avg_response_days,
        lag(count(*)) over (partition by season order by year) as prev_year_complaints,
        round((count(*) - lag(count(*)) over (partition by season order by year)) * 100.0 / 
            nullif(lag(count(*)) over (partition by season order by year), 0), 2) as yoy_growth_rate
    from complaint_seasonal_data
    group by year, season
),

-- ============================================
-- WEEKLY PATTERNS WITHIN SEASONS
-- ============================================
weekly_seasonal_patterns as (
    select
        season,
        week_of_year,
        count(*) as weekly_complaints,
        count(case when is_closed = true then 1 end) as weekly_closed,
        avg(count(*)) over (partition by season) as season_weekly_avg,
        round(avg(valid_response_time_days), 2) as weekly_avg_response_days
    from complaint_seasonal_data
    group by season, week_of_year
),

-- ============================================
-- FINAL ASSEMBLY
-- ============================================
final as (
    select
        mp.month,
        mp.month_name,
        mp.season,
        mp.nyc_seasonal_context,
        
        -- Volume analysis
        mp.total_complaints,
        round(mp.total_complaints * 1.0 / mp.years_in_data, 2) as avg_annual_monthly_complaints,
        mp.annual_avg_complaints,
        round((mp.total_complaints - mp.annual_avg_complaints) * 100.0 / nullif(mp.annual_avg_complaints, 0), 2) as monthly_deviation_pct,
        
        sa.season_avg_complaints,
        round((mp.total_complaints - sa.season_avg_complaints) * 100.0 / nullif(sa.season_avg_complaints, 0), 2) as seasonal_deviation_pct,
        
        -- Closure metrics
        mp.closed_complaints,
        mp.closure_rate,
        round(sa.season_avg_closure_rate, 2) as season_avg_closure_rate,
        
        -- ✅✅ NEW: Enhanced data quality metrics (separate for response time and SLA)
        mp.complaints_with_valid_response_time,
        mp.response_time_data_quality_pct,
        mp.total_valid_for_sla,
        mp.sla_data_quality_pct,
        round(sa.season_avg_response_quality, 2) as season_avg_response_quality,
        round(sa.season_avg_sla_quality, 2) as season_avg_sla_quality,
        mp.invalid_closed_dates,
        mp.historical_closed_dates,
        mp.future_closed_dates,
        round((mp.invalid_closed_dates + mp.historical_closed_dates + mp.future_closed_dates) * 100.0 / 
            nullif(mp.total_complaints, 0), 2) as total_invalid_date_pct,
        
        -- ✅✅ NEW: Separate data quality tiers
        case 
            when mp.response_time_data_quality_pct >= 80 then 'HIGH'
            when mp.response_time_data_quality_pct >= 60 then 'MEDIUM'
            else 'LOW'
        end as response_time_quality_tier,
        
        case 
            when mp.sla_data_quality_pct >= 80 then 'HIGH'
            when mp.sla_data_quality_pct >= 60 then 'MEDIUM'
            else 'LOW'
        end as sla_quality_tier,
        
        -- Category breakdowns with seasonal context
        mp.noise_complaints,
        round(mp.noise_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as noise_percentage,
        round(sa.season_avg_noise, 2) as season_avg_noise,
        
        mp.housing_complaints,  
        round(mp.housing_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as housing_percentage,
        round(sa.season_avg_housing, 2) as season_avg_housing,
        
        mp.parks_complaints,
        round(mp.parks_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as parks_percentage,
        
        mp.street_complaints,
        round(mp.street_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as street_percentage,
        
        mp.transportation_complaints,
        round(mp.transportation_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as transportation_percentage,
        
        mp.sanitation_complaints,
        round(mp.sanitation_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as sanitation_percentage,
        
        mp.quality_of_life_complaints,
        round(mp.quality_of_life_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as quality_of_life_percentage,
        
        -- Weather-related seasonal indicators
        mp.heating_complaints,
        round(mp.heating_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as heating_complaint_pct,
        round(sa.season_avg_heating, 2) as season_avg_heating,
        
        mp.winter_weather_complaints,
        round(mp.winter_weather_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as winter_weather_pct,
        round(sa.season_avg_winter_weather, 2) as season_avg_winter_weather,
        
        mp.cooling_complaints,
        round(mp.cooling_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as cooling_complaint_pct,
        round(sa.season_avg_cooling, 2) as season_avg_cooling,
        
        mp.tree_complaints,
        round(mp.tree_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as tree_complaint_pct,
        
        mp.street_condition_complaints,
        round(mp.street_condition_complaints * 100.0 / nullif(mp.total_complaints, 0), 2) as street_condition_pct,
        
        -- Performance seasonality (hours and days)
        round(mp.avg_response_time_hours, 2) as avg_response_time_hours,
        round(mp.avg_response_time_days, 2) as avg_response_time_days,
        round(mp.median_response_time_hours, 2) as median_response_time_hours,
        round(mp.median_response_time_days, 2) as median_response_time_days,
        round(sa.season_avg_response_time_hours, 2) as season_avg_response_time_hours,
        round(sa.season_avg_response_time_days, 2) as season_avg_response_time_days,
        
        -- SLA metrics
        mp.same_day_closure_rate,
        round(sa.season_avg_same_day_rate, 2) as season_avg_same_day_rate,
        mp.within_week_rate,
        round(sa.season_avg_within_week_rate, 2) as season_avg_within_week_rate,
        mp.within_month_rate,
        mp.over_month_rate,
        
        -- Seasonal pattern indicators
        case
            when mp.month in (1, 2, 12) and mp.heating_complaints > mp.total_complaints * 0.15 then 'High Winter Demand'
            when mp.month in (6, 7, 8) and mp.noise_complaints > mp.total_complaints * 0.3 then 'Summer Noise Peak'
            when mp.month in (4, 5) and mp.tree_complaints > mp.total_complaints * 0.05 then 'Spring Maintenance Season'
            when mp.month in (9, 10) and mp.street_condition_complaints > mp.total_complaints * 0.1 then 'Fall Infrastructure Focus'
            when mp.month in (1, 2) and mp.winter_weather_complaints > mp.total_complaints * 0.05 then 'Winter Weather Impact'
            else 'Normal Seasonal Pattern'
        end as seasonal_pattern_indicator,
        
        -- Seasonality strength score
        round(abs(mp.total_complaints - mp.annual_avg_complaints) * 100.0 / nullif(mp.annual_avg_complaints, 0), 2) as seasonality_strength,
        
        -- Seasonality classification
        case
            when abs(mp.total_complaints - mp.annual_avg_complaints) * 100.0 / nullif(mp.annual_avg_complaints, 0) > 20 then 'High Seasonality'
            when abs(mp.total_complaints - mp.annual_avg_complaints) * 100.0 / nullif(mp.annual_avg_complaints, 0) > 10 then 'Moderate Seasonality'
            else 'Low Seasonality'
        end as seasonality_level,
        
        -- Climate adaptation indicators
        case
            when mp.heating_complaints + mp.cooling_complaints > mp.total_complaints * 0.2 then 'Climate Sensitive'
            when mp.winter_weather_complaints + mp.tree_complaints > mp.total_complaints * 0.1 then 'Weather Sensitive'
            else 'Climate Stable'
        end as climate_sensitivity,
        
        -- Peak indicator
        case
            when mp.total_complaints > mp.annual_avg_complaints * 1.15 then 'Peak Month'
            when mp.total_complaints < mp.annual_avg_complaints * 0.85 then 'Trough Month'
            else 'Average Month'
        end as volume_classification,
        
        -- Metadata
        current_timestamp() as created_at
        
    from monthly_patterns mp
    left join seasonal_averages sa on mp.season = sa.season
)

select * from final
order by month

-- ========================================
-- VALIDATION QUERIES
-- ========================================
/*
-- 1. Seasonal summary with separate quality metrics
SELECT 
    season,
    SUM(total_complaints) as total_seasonal_complaints,
    AVG(avg_annual_monthly_complaints) as avg_monthly_complaints,
    AVG(closure_rate) as avg_closure_rate,
    AVG(noise_percentage) as avg_noise_pct,
    AVG(housing_percentage) as avg_housing_pct,
    AVG(heating_complaint_pct) as avg_heating_pct,
    AVG(cooling_complaint_pct) as avg_cooling_pct,
    AVG(avg_response_time_days) as avg_response_days,
    AVG(same_day_closure_rate) as avg_same_day_rate,
    AVG(response_time_data_quality_pct) as avg_response_quality,
    AVG(sla_data_quality_pct) as avg_sla_quality,
    COUNT(CASE WHEN seasonality_level = 'High Seasonality' THEN 1 END) as high_seasonality_months,
    COUNT(CASE WHEN response_time_quality_tier = 'HIGH' THEN 1 END) as high_response_quality_months,
    COUNT(CASE WHEN sla_quality_tier = 'HIGH' THEN 1 END) as high_sla_quality_months
FROM {{ this }}
GROUP BY season
ORDER BY 
    CASE season 
        WHEN 'Winter' THEN 1 
        WHEN 'Spring' THEN 2 
        WHEN 'Summer' THEN 3 
        WHEN 'Fall' THEN 4 
    END;

-- 2. High quality SLA analysis by season
SELECT 
    season,
    AVG(same_day_closure_rate) as avg_same_day_rate,
    AVG(within_week_rate) as avg_within_week_rate,
    AVG(within_month_rate) as avg_within_month_rate,
    AVG(over_month_rate) as avg_over_month_rate,
    AVG(sla_data_quality_pct) as avg_sla_quality,
    COUNT(*) as month_count
FROM {{ this }}
WHERE sla_quality_tier = 'HIGH'  -- Only high quality SLA data
GROUP BY season
ORDER BY 
    CASE season 
        WHEN 'Winter' THEN 1 
        WHEN 'Spring' THEN 2 
        WHEN 'Summer' THEN 3 
        WHEN 'Fall' THEN 4 
    END;

-- 3. Response time vs SLA quality comparison by season
SELECT 
    season,
    response_time_quality_tier,
    sla_quality_tier,
    COUNT(*) as month_count,
    AVG(avg_response_time_days) as avg_response_days,
    AVG(same_day_closure_rate) as avg_same_day_rate,
    AVG(total_complaints) as avg_complaints
FROM {{ this }}
GROUP BY season, response_time_quality_tier, sla_quality_tier
ORDER BY season, response_time_quality_tier, sla_quality_tier;

-- 4. Month-by-month with quality tiers
SELECT 
    month,
    month_name,
    season,
    total_complaints,
    closure_rate,
    same_day_closure_rate,
    response_time_data_quality_pct,
    sla_data_quality_pct,
    response_time_quality_tier,
    sla_quality_tier,
    total_valid_for_sla,
    seasonal_pattern_indicator
FROM {{ this }}
ORDER BY month;

-- 5. Performance seasonality (VALID SLA DATA ONLY)
SELECT 
    season,
    AVG(same_day_closure_rate) as avg_same_day_rate,
    AVG(within_week_rate) as avg_within_week_rate,
    AVG(within_month_rate) as avg_within_month_rate,
    AVG(closure_rate) as avg_closure_rate,
    AVG(avg_response_time_days) as avg_response_days,
    AVG(sla_data_quality_pct) as avg_sla_quality,
    SUM(total_valid_for_sla) as total_valid_sla_cases
FROM {{ this }}
WHERE sla_quality_tier IN ('HIGH', 'MEDIUM')  -- Exclude low quality
GROUP BY season
ORDER BY 
    CASE season 
        WHEN 'Winter' THEN 1 
        WHEN 'Spring' THEN 2 
        WHEN 'Summer' THEN 3 
        WHEN 'Fall' THEN 4 
    END;

-- 6. Data quality assessment by season
SELECT 
    season,
    AVG(response_time_data_quality_pct) as avg_response_quality,
    AVG(sla_data_quality_pct) as avg_sla_quality,
    AVG(total_invalid_date_pct) as avg_invalid_date_pct,
    SUM(invalid_closed_dates) as total_invalid,
    SUM(historical_closed_dates) as total_historical,
    SUM(future_closed_dates) as total_future,
    COUNT(CASE WHEN response_time_quality_tier = 'HIGH' THEN 1 END) as high_response_months,
    COUNT(CASE WHEN sla_quality_tier = 'HIGH' THEN 1 END) as high_sla_months
FROM {{ this }}
GROUP BY season
ORDER BY 
    CASE season 
        WHEN 'Winter' THEN 1 
        WHEN 'Spring' THEN 2 
        WHEN 'Summer' THEN 3 
        WHEN 'Fall' THEN 4 
    END;

-- 7. Peak months with quality filters
SELECT 
    month_name,
    season,
    volume_classification,
    total_complaints,
    monthly_deviation_pct,
    same_day_closure_rate,
    sla_data_quality_pct,
    sla_quality_tier,
    total_valid_for_sla
FROM {{ this }}
WHERE volume_classification IN ('Peak Month', 'Trough Month')
  AND sla_quality_tier IN ('HIGH', 'MEDIUM')  -- Only reliable data
ORDER BY total_complaints DESC;
*/

/*
✅✅ KEY IMPROVEMENTS FROM ORIGINAL:
====================================
1. Created `valid_sla_category` field (excludes INVALID_DATE, NEGATIVE_TIME, NO_DATE, OPEN)
2. Added `>= 0` check for response times (exclude negative values)
3. Fixed ALL SLA calculations to use `valid_sla_category` instead of raw `response_sla_category`
4. Changed SLA denominators from "all closed" to "valid SLA category only"
5. Added `total_valid_for_sla` tracking metric
6. Added `sla_data_quality_pct` (separate from response time quality)
7. Created separate quality tiers: `response_time_quality_tier` and `sla_quality_tier`
8. Updated seasonal averages to include both quality metrics */
