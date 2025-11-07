-- Location Metrics Aggregation Table - OPTIMIZED FOR WATERMARK-BASED FACT
{{ config(
    materialized='incremental',
    unique_key=['date_key', 'location_key'],
    incremental_strategy='merge',
    on_schema_change='fail',
    tags=['aggregation', 'daily', 'location', 'geographic']
) }}

{% if is_incremental() %}
    -- Extended lookback for rolling averages (7 days + 7 days window)
    {% set lookback_days = var('incremental_lookback_days', 7) + 7 %}

    {% set min_date_key_query %}
        select cast(
            date_format(
                date_sub(max(d.date_actual), {{ lookback_days }}), 
                'yyyyMMdd'
            ) as int
        )
        from {{ this }} t
        join {{ ref('dim_date') }} d on t.date_key = d.date_key
    {% endset %}
    {% set min_date_key = run_query(min_date_key_query).columns[0][0] %}
{% endif %}

-- ============================================
-- SINGLE BASE SCAN: Get all data in one pass WITH FLAGS
-- ============================================
with base_data as (
    select
        f.created_date_key as date_key,
        f.location_key,
        f.status,
        f.is_closed,

        -- Response time (only if valid)
        f.borough,

        -- SLA category (only valid ones)
        f.city,

        f.complaint_type,
        d.date_actual,
        case
            when f.is_closed
                and not f.has_invalid_closed_date
                and not f.has_historical_closed_date
                and not f.has_future_closed_date
                and f.response_time_hours is not null
                and f.response_time_hours >= 0
            then f.response_time_hours
        end as valid_response_time_hours,
        case
            when f.response_sla_category in ('INVALID_DATE', 'NEGATIVE_TIME', 'NO_DATE') then null
            else f.response_sla_category
        end as valid_sla_category

    from {{ ref('fact_311') }} f
    inner join {{ ref('dim_date') }} d
        on f.created_date_key = d.date_key

    {% if is_incremental() %}
    -- Use created_date_key for partition pruning
    where f.created_date_key >= {{ min_date_key }}
    {% endif %}
),

-- ============================================
-- LOCATION ATTRIBUTES: Get zip from dim_location
-- ============================================
location_attrs as (
    select
        location_id as location_key,
        zip_code
    from {{ ref('dim_location') }}
),

-- ============================================
-- MAIN AGGREGATION: All location metrics
-- ============================================
location_daily as (
    select
        date_key,
        location_key,
        max(borough) as borough,
        max(city) as city,
        max(date_actual) as date_actual,

        -- Complaint counts
        count(*) as total_complaints,
        sum(case when status = 'OPEN' then 1 else 0 end) as open_complaints,
        sum(case when is_closed then 1 else 0 end) as closed_complaints,
        count(*) as new_complaints,

        -- Response metrics (only VALID data)
        avg(valid_response_time_hours) as avg_response_time_hours,
        percentile_approx(valid_response_time_hours, 0.5) as median_response_time_hours,
        count(valid_response_time_hours) as valid_response_count,

        -- SLA metrics (only VALID data)
        sum(case when valid_sla_category in ('SAME_DAY', 'WITHIN_WEEK') then 1 else 0 end) as within_sla_count,
        count(valid_sla_category) as total_valid_closed_for_sla,

        -- Data quality tracking
        sum(case when is_closed and valid_response_time_hours is null then 1 else 0 end) as closed_without_valid_time

    from base_data
    group by date_key, location_key
),

-- ============================================
-- TOP COMPLAINT TYPES (Window function)
-- ============================================
complaint_type_rankings as (
    select
        date_key,
        location_key,
        complaint_type,
        count(*) as complaint_count,
        row_number() over (
            partition by date_key, location_key
            order by count(*) desc, complaint_type
        ) as rn
    from base_data
    where complaint_type is not null
    group by date_key, location_key, complaint_type
),

top_complaints as (
    select
        date_key,
        location_key,
        max(case when rn = 1 then complaint_type end) as top_complaint_type_1,
        max(case when rn = 1 then complaint_count end) as top_complaint_type_1_count,
        max(case when rn = 2 then complaint_type end) as top_complaint_type_2,
        max(case when rn = 2 then complaint_count end) as top_complaint_type_2_count,
        max(case when rn = 3 then complaint_type end) as top_complaint_type_3,
        max(case when rn = 3 then complaint_count end) as top_complaint_type_3_count
    from complaint_type_rankings
    where rn <= 3
    group by date_key, location_key
),

-- ============================================
-- ROLLING METRICS (7-day average)
-- ============================================
rolling_avg as (
    select
        date_key,
        location_key,
        avg(total_complaints) over (
            partition by location_key
            order by date_key
            rows between 6 preceding and current row
        ) as complaints_7day_avg,
        lag(total_complaints, 7) over (
            partition by location_key
            order by date_key
        ) as prev_week_complaints,
        lag(total_complaints, 1) over (
            partition by location_key
            order by date_key
        ) as prev_day_complaints
    from location_daily
),

-- ============================================
-- BOROUGH AVERAGES (for hotspot detection)
-- ============================================
borough_avg as (
    select
        date_key,
        borough,
        avg(total_complaints) as borough_daily_avg,
        count(distinct location_key) as locations_in_borough,
        sum(total_complaints) as total_borough_complaints
    from location_daily
    group by date_key, borough
),

-- ============================================
-- LOCATION RANKINGS
-- ============================================
location_rankings as (
    select
        date_key,
        location_key,
        borough,
        city,
        total_complaints,
        row_number() over (
            partition by date_key, borough
            order by total_complaints desc
        ) as borough_rank_daily,
        row_number() over (
            partition by date_key, borough, city
            order by total_complaints desc
        ) as city_rank_in_borough,
        row_number() over (
            partition by date_key
            order by total_complaints desc
        ) as citywide_rank
    from location_daily
),

-- ============================================
-- FINAL ASSEMBLY
-- ============================================
final as (
    select
        -- Keys & Location Info
        ld.date_key,
        ld.location_key,
        ld.borough,
        ld.city,
        la.zip_code,

        -- Volume metrics
        ld.total_complaints,
        ld.open_complaints,
        ld.closed_complaints,
        ld.new_complaints,

        -- Performance metrics (ONLY from valid data)
        ld.valid_response_count,
        ld.total_valid_closed_for_sla,
        ld.closed_without_valid_time,
        tc.top_complaint_type_1,
        tc.top_complaint_type_1_count,
        tc.top_complaint_type_2,

        -- Data quality metrics
        tc.top_complaint_type_2_count,
        tc.top_complaint_type_3,

        -- Top 3 complaint types
        tc.top_complaint_type_3_count,
        lr.borough_rank_daily,
        lr.city_rank_in_borough,
        lr.citywide_rank,
        ba.locations_in_borough,
        round(ld.avg_response_time_hours, 2) as avg_response_time_hours,

        -- Trend metrics
        round(ld.median_response_time_hours, 2) as median_response_time_hours,
        round(ld.within_sla_count * 100.0 / nullif(ld.total_valid_closed_for_sla, 0), 2) as sla_compliance_rate,
        round(ld.closed_complaints * 100.0 / nullif(ld.total_complaints, 0), 2) as completion_rate,

        -- Hotspot detection (1.5x borough average)
        round(ld.closed_without_valid_time * 100.0 / nullif(ld.closed_complaints, 0), 2) as pct_closed_missing_valid_time,

        -- Hotspot severity (2x = high, 1.5-2x = medium)
        round(ra.complaints_7day_avg, 2) as complaints_7day_avg,

        -- Rankings
        round((ld.total_complaints - coalesce(ra.prev_week_complaints, 0)) * 100.0
            / nullif(ra.prev_week_complaints, 0), 2) as complaints_vs_prev_week_pct,
        round((ld.total_complaints - coalesce(ra.prev_day_complaints, 0)) * 100.0
            / nullif(ra.prev_day_complaints, 0), 2) as complaints_vs_prev_day_pct,
        coalesce(ld.total_complaints > ba.borough_daily_avg * 1.5, false) as is_hotspot,

        -- Context metrics
        case
            when ba.borough_daily_avg is null then 'NO_DATA'
            when ld.total_complaints > ba.borough_daily_avg * 2.0 then 'HIGH'
            when ld.total_complaints > ba.borough_daily_avg * 1.5 then 'MEDIUM'
            when ld.total_complaints > ba.borough_daily_avg * 1.2 then 'LOW'
            else 'NORMAL'
        end as hotspot_severity,
        round(ba.borough_daily_avg, 2) as borough_avg_complaints,
        round(ld.total_complaints * 100.0 / nullif(ba.borough_daily_avg, 0), 2) as pct_of_borough_avg,
        round(ld.total_complaints * 100.0 / nullif(ba.total_borough_complaints, 0), 2) as pct_of_borough_total,

        -- Trend indicators
        case
            when ra.prev_week_complaints is null then 'NO_DATA'
            when ld.total_complaints > ra.prev_week_complaints * 1.2 then 'INCREASING'
            when ld.total_complaints < ra.prev_week_complaints * 0.8 then 'DECREASING'
            else 'STABLE'
        end as trend_direction,

        -- Volatility indicator
        case
            when ra.complaints_7day_avg is null then 'NO_DATA'
            when abs(ld.total_complaints - ra.complaints_7day_avg) > ra.complaints_7day_avg * 0.3 then 'HIGH_VARIANCE'
            when abs(ld.total_complaints - ra.complaints_7day_avg) > ra.complaints_7day_avg * 0.15 then 'MEDIUM_VARIANCE'
            else 'LOW_VARIANCE'
        end as volatility,

        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from location_daily ld
    left join location_attrs la
        on ld.location_key = la.location_key
    left join rolling_avg ra
        on ld.date_key = ra.date_key
        and ld.location_key = ra.location_key
    left join borough_avg ba
        on ld.date_key = ba.date_key
        and ld.borough = ba.borough
    left join location_rankings lr
        on ld.date_key = lr.date_key
        and ld.location_key = lr.location_key
    left join top_complaints tc
        on ld.date_key = tc.date_key
        and ld.location_key = tc.location_key
)

select * from final
order by date_key desc, total_complaints desc
