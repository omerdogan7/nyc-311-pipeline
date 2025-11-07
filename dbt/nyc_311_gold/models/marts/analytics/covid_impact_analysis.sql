-- COVID-19 Impact Analysis on NYC 311 Complaints - ALIGNED WITH FACT TABLE
{{ 
  config(
    materialized='table',
    tags=['analytics', 'covid', 'research']
  ) 
}}

with date_periods as (
    select
        date_key,
        date_actual,
        year,
        month,
        month_name,
        case
            when date_actual < date('2020-03-16') then 'Pre-COVID'
            when date_actual between date('2020-03-16') and date('2021-06-30') then 'COVID Peak'
            when date_actual > date('2021-06-30') then 'Post-COVID'
        end as covid_period,
        coalesce(date_actual between date('2020-03-16') and date('2021-06-30'), false) as is_covid_period
    from {{ ref('dim_date') }}
    where year between 2019 and 2023
),

complaint_base as (
    select
        f.unique_key,
        f.created_date_key,
        f.closed_date_key,
        f.status,
        f.is_closed,

        -- Valid response time fields with data quality flags
        f.borough,

        f.agency_code,

        -- Use validated SLA category (exclude invalid categories)
        f.complaint_type,

        f.descriptor,
        f.has_invalid_closed_date,
        f.has_historical_closed_date,
        f.has_future_closed_date,

        -- Data quality flags for tracking
        f.has_closed_status_without_date,
        f.open_status_with_closed_date,
        dp.covid_period,
        dp.is_covid_period,
        dp.year,

        dp.month,
        dp.month_name,
        dp.date_actual,
        ct.complaint_category,
        case
            when f.is_closed
                and not f.has_invalid_closed_date
                and not f.has_historical_closed_date
                and not f.has_future_closed_date
                and f.response_time_hours is not null
                and f.response_time_hours >= 0  -- Added: Exclude negative values
            then f.response_time_hours
        end as valid_response_time_hours,
        case
            when f.is_closed
                and not f.has_invalid_closed_date
                and not f.has_historical_closed_date
                and not f.has_future_closed_date
                and f.response_time_days is not null
                and f.response_time_days >= 0  -- Added: Exclude negative values
            then f.response_time_days
        end as valid_response_time_days,
        case
            when f.response_sla_category in ('INVALID_DATE', 'NEGATIVE_TIME', 'NO_DATE', 'OPEN') then null
            else f.response_sla_category
        end as valid_sla_category

    from {{ ref('fact_311') }} f
    inner join date_periods dp
        on f.created_date_key = dp.date_key
    left join {{ ref('dim_complaint_type') }} ct
        on f.complaint_type_key = ct.complaint_type_key
),

-- ============================================
-- MONTHLY AGGREGATIONS BY COVID PERIOD
-- ============================================
monthly_covid_impact as (
    select
        year,
        month,
        covid_period,
        max(month_name) as month_name,

        -- Volume metrics
        count(*) as total_complaints,
        count(case when is_closed then 1 end) as closed_complaints,

        -- Response time metrics (VALID data only)
        count(case when is_closed and valid_response_time_hours is not null then 1 end) as complaints_with_valid_response_time,
        avg(valid_response_time_hours) as avg_response_time_hours,
        avg(valid_response_time_days) as avg_response_time_days,
        percentile_approx(valid_response_time_hours, 0.5) as median_response_time_hours,
        percentile_approx(valid_response_time_days, 0.5) as median_response_time_days,

        -- SLA metrics (VALID categories only)
        round(count(case when valid_sla_category = 'SAME_DAY' then 1 end) * 100.0
            / nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as same_day_closure_rate,
        round(count(case when valid_sla_category = 'WITHIN_WEEK' then 1 end) * 100.0
            / nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as within_week_rate,
        round(count(case when valid_sla_category = 'WITHIN_MONTH' then 1 end) * 100.0
            / nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as within_month_rate,
        round(count(case when valid_sla_category = 'OVER_MONTH' then 1 end) * 100.0
            / nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as over_month_rate,
        count(case when valid_sla_category is not null then 1 end) as total_valid_for_sla,

        -- Data quality metrics
        count(case when has_invalid_closed_date then 1 end) as invalid_closed_dates,
        count(case when has_closed_status_without_date then 1 end) as closed_without_date,
        count(case when open_status_with_closed_date then 1 end) as open_with_closed_date,
        round(count(case when is_closed and valid_response_time_hours is not null then 1 end) * 100.0
            / nullif(count(case when is_closed then 1 end), 0), 2) as response_time_data_quality_pct,

        -- Category breakdowns
        count(case when complaint_category = 'Noise' then 1 end) as noise_complaints,
        count(case when complaint_category = 'Transportation' then 1 end) as transportation_complaints,
        count(case when complaint_category = 'Building & Housing' then 1 end) as housing_complaints,
        count(case when complaint_category = 'Sanitation & Cleanliness' then 1 end) as sanitation_complaints,
        count(case when complaint_category = 'Quality of Life' then 1 end) as quality_of_life_complaints,

        -- Specific COVID-related patterns
        count(case when upper(complaint_type) like '%COVID%' then 1 end) as covid_specific_complaints,
        count(case when upper(complaint_type) like '%NOISE - RESIDENTIAL%' then 1 end) as residential_noise_complaints,
        count(case when upper(complaint_type) like '%OUTDOOR DINING%' then 1 end) as outdoor_dining_complaints,
        count(case when upper(complaint_type) like '%ILLEGAL PARKING%' then 1 end) as illegal_parking_complaints

    from complaint_base
    group by year, month, covid_period
),

-- ============================================
-- PRE-COVID BASELINE (2019 average)
-- ============================================
pre_covid_baseline as (
    select
        month,
        avg(total_complaints) as baseline_monthly_complaints,
        avg(noise_complaints) as baseline_noise_complaints,
        avg(transportation_complaints) as baseline_transportation_complaints,
        avg(housing_complaints) as baseline_housing_complaints,
        avg(avg_response_time_hours) as baseline_response_time_hours,
        avg(avg_response_time_days) as baseline_response_time_days,
        avg(same_day_closure_rate) as baseline_same_day_rate,
        avg(within_week_rate) as baseline_within_week_rate
    from monthly_covid_impact
    where covid_period = 'Pre-COVID' and year = 2019
    group by month
),

-- ============================================
-- BOROUGH-LEVEL COVID IMPACT
-- ============================================
borough_covid_impact as (
    select
        borough,
        covid_period,
        count(*) as complaints_count,
        count(case when is_closed then 1 end) as closed_count,
        round(count(case when is_closed then 1 end) * 100.0 / nullif(count(*), 0), 2) as closure_rate,
        round(avg(valid_response_time_hours), 2) as avg_response_time_hours,
        round(avg(valid_response_time_days), 2) as avg_response_time_days,
        count(case when complaint_category = 'Noise' then 1 end) as noise_count,
        round(count(case when complaint_category = 'Noise' then 1 end) * 100.0 / nullif(count(*), 0), 2) as noise_percentage,
        count(case when complaint_category = 'Transportation' then 1 end) as transportation_count,
        round(count(case when complaint_category = 'Transportation' then 1 end) * 100.0 / nullif(count(*), 0), 2) as transportation_percentage
    from complaint_base
    where borough != 'UNSPECIFIED'
    group by borough, covid_period
),

-- ============================================
-- AGENCY-LEVEL COVID IMPACT
-- ============================================
agency_covid_impact as (
    select
        agency_code,
        covid_period,
        count(*) as complaints_count,
        count(case when is_closed then 1 end) as closed_count,
        round(avg(valid_response_time_hours), 2) as avg_response_time_hours,
        round(avg(valid_response_time_days), 2) as avg_response_time_days,
        round(count(case when is_closed then 1 end) * 100.0 / nullif(count(*), 0), 2) as closure_rate,
        round(count(case when valid_sla_category = 'SAME_DAY' then 1 end) * 100.0
            / nullif(count(case when valid_sla_category is not null then 1 end), 0), 2) as same_day_rate
    from complaint_base
    where agency_code is not null and agency_code != 'UNKNOWN'
    group by agency_code, covid_period
),

-- ============================================
-- TOP COMPLAINT TYPES BY PERIOD
-- ============================================
top_complaints_by_period as (
    select
        covid_period,
        complaint_type,
        count(*) as complaint_count,
        count(case when is_closed then 1 end) as closed_count,
        round(avg(valid_response_time_days), 2) as avg_days_to_close,
        row_number() over (partition by covid_period order by count(*) desc) as rank_in_period
    from complaint_base
    where complaint_type is not null and complaint_type != 'UNKNOWN'
    group by covid_period, complaint_type
),

-- ============================================
-- FINAL ASSEMBLY
-- ============================================
final as (
    select
        mci.year,
        mci.month,
        mci.month_name,
        mci.covid_period,

        -- Volume metrics
        mci.total_complaints,
        pcb.baseline_monthly_complaints,
        mci.closed_complaints,
        mci.complaints_with_valid_response_time,

        -- Closure metrics
        mci.response_time_data_quality_pct,
        mci.avg_response_time_hours,

        -- Response time metrics with data quality
        mci.avg_response_time_days,
        mci.median_response_time_hours,
        mci.median_response_time_days,
        pcb.baseline_response_time_hours,
        pcb.baseline_response_time_days,
        mci.total_valid_for_sla,
        mci.same_day_closure_rate,
        pcb.baseline_same_day_rate,
        mci.within_week_rate,
        pcb.baseline_within_week_rate,

        -- SLA metrics using valid categories
        mci.within_month_rate,
        mci.over_month_rate,
        mci.invalid_closed_dates,
        mci.closed_without_date,

        mci.open_with_closed_date,
        mci.noise_complaints,
        pcb.baseline_noise_complaints,

        mci.transportation_complaints,
        pcb.baseline_transportation_complaints,

        -- Data quality metrics
        mci.housing_complaints,
        mci.sanitation_complaints,
        mci.quality_of_life_complaints,
        mci.covid_specific_complaints,

        -- Category shifts
        mci.residential_noise_complaints,
        mci.outdoor_dining_complaints,
        mci.illegal_parking_complaints,
        round((mci.total_complaints - pcb.baseline_monthly_complaints) * 100.0
            / nullif(pcb.baseline_monthly_complaints, 0), 2) as volume_change_pct,

        mci.total_complaints - coalesce(pcb.baseline_monthly_complaints, 0) as volume_change_absolute,
        round(mci.closed_complaints * 100.0 / nullif(mci.total_complaints, 0), 2) as closure_rate,
        round((mci.avg_response_time_hours - pcb.baseline_response_time_hours) * 100.0
            / nullif(pcb.baseline_response_time_hours, 0), 2) as response_time_hours_change_pct,
        round((mci.avg_response_time_days - pcb.baseline_response_time_days) * 100.0
            / nullif(pcb.baseline_response_time_days, 0), 2) as response_time_days_change_pct,

        round(mci.same_day_closure_rate - pcb.baseline_same_day_rate, 2) as same_day_rate_change_points,
        round(mci.within_week_rate - pcb.baseline_within_week_rate, 2) as within_week_rate_change_points,

        round(mci.invalid_closed_dates * 100.0 / nullif(mci.total_complaints, 0), 2) as invalid_date_pct,
        round((mci.noise_complaints - pcb.baseline_noise_complaints) * 100.0
            / nullif(pcb.baseline_noise_complaints, 0), 2) as noise_change_pct,

        round(mci.noise_complaints * 100.0 / nullif(mci.total_complaints, 0), 2) as noise_mix_pct,
        round((mci.transportation_complaints - pcb.baseline_transportation_complaints) * 100.0
            / nullif(pcb.baseline_transportation_complaints, 0), 2) as transportation_change_pct,

        -- COVID-specific metrics
        round(mci.transportation_complaints * 100.0 / nullif(mci.total_complaints, 0), 2) as transportation_mix_pct,
        round(mci.housing_complaints * 100.0 / nullif(mci.total_complaints, 0), 2) as housing_mix_pct,
        round(mci.sanitation_complaints * 100.0 / nullif(mci.total_complaints, 0), 2) as sanitation_mix_pct,
        round(mci.quality_of_life_complaints * 100.0 / nullif(mci.total_complaints, 0), 2) as quality_of_life_mix_pct,

        -- Pattern indicators
        case
            when mci.noise_complaints > coalesce(pcb.baseline_noise_complaints, 0) * 1.3 then 'High Noise Period'
            when mci.transportation_complaints < coalesce(pcb.baseline_transportation_complaints, mci.transportation_complaints) * 0.7 then 'Low Transit Period'
            when mci.total_complaints < coalesce(pcb.baseline_monthly_complaints, mci.total_complaints) * 0.85 then 'Low Activity Period'
            when mci.residential_noise_complaints > mci.noise_complaints * 0.5 then 'WFH Impact High'
            else 'Normal Pattern'
        end as covid_pattern_indicator,

        -- WFH impact score (proxy: residential noise vs transportation ratio)
        round(mci.noise_complaints * 1.0 / nullif(mci.transportation_complaints, 1), 2) as wfh_impact_score,

        -- Seasonality adjustment flag
        case
            when mci.month in (6, 7, 8) then 'Summer'
            when mci.month in (12, 1, 2) then 'Winter'
            when mci.month in (3, 4, 5) then 'Spring'
            when mci.month in (9, 10, 11) then 'Fall'
        end as season,

        -- COVID phase classification
        case
            when mci.covid_period = 'COVID Peak' and mci.year = 2020 and mci.month between 3 and 6 then 'Lockdown Phase'
            when mci.covid_period = 'COVID Peak' and mci.year = 2020 and mci.month between 7 and 12 then 'Early Recovery'
            when mci.covid_period = 'COVID Peak' and mci.year = 2021 then 'Late Recovery'
            when mci.covid_period = 'Post-COVID' then 'New Normal'
            when mci.covid_period = 'Pre-COVID' then 'Baseline'
        end as covid_phase,

        -- Metadata
        current_timestamp() as created_at

    from monthly_covid_impact mci
    left join pre_covid_baseline pcb on mci.month = pcb.month
)

select * from final
order by year, month
