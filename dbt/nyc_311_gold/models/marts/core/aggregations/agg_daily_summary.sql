-- Daily Summary Aggregation Table - OPTIMIZED FOR WATERMARK-BASED FACT
{{ 
  config(
    materialized='incremental',
    unique_key='date_key',
    on_schema_change='fail',
    tags=['aggregation', 'daily', 'reporting']
  ) 
}}

{% if is_incremental() %}
    -- ✅ Rolling averages için extended lookback (7 gün + 30 gün window)
    {% set lookback_days = var('incremental_lookback_days', 7) + 30 %}

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

with base_data as (
    -- ============================================
    -- SINGLE TABLE SCAN: Fetch all data once WITH FLAGS
    -- ============================================
    select
        f.created_date_key,
        f.closed_date_key,
        f.status,
        f.is_closed,

        -- ✅ Response time (only if valid)
        f.agency_code,

        -- ✅ SLA category (only valid ones)
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
    -- ✅ Sadece created_date_key kullan (partition pruning için)
    where f.created_date_key >= {{ min_date_key }}
    {% endif %}
),

-- ============================================
-- MAIN AGGREGATION: All metrics in ONE pass
-- ============================================
daily_metrics as (
    select
        created_date_key as date_key,
        max(date_actual) as date_actual,

        -- Total counts
        count(*) as total_complaints,

        -- Status breakdown
        sum(case when is_closed then 1 else 0 end) as completed_count,
        sum(case when status = 'OPEN' then 1 else 0 end) as new_count,
        sum(case when status in ('ASSIGNED', 'STARTED', 'IN PROGRESS') then 1 else 0 end) as in_progress_count,
        sum(case when status = 'PENDING' then 1 else 0 end) as waiting_count,
        sum(case when status in ('CANCEL', 'UNSPECIFIED') then 1 else 0 end) as other_status_count,

        -- ✅ Response time metrics (only VALID data)
        avg(valid_response_time_hours) as avg_response_time_hours,
        percentile_approx(valid_response_time_hours, 0.5) as median_response_time_hours,
        count(valid_response_time_hours) as valid_response_count,

        -- ✅ SLA metrics (only VALID data)
        sum(case when valid_sla_category in ('SAME_DAY', 'WITHIN_WEEK') then 1 else 0 end) as within_sla_count,
        count(valid_sla_category) as total_valid_closed_for_sla,

        -- ✅ Data quality tracking
        sum(case when is_closed and valid_response_time_hours is null then 1 else 0 end) as closed_without_valid_time,
        sum(case when valid_sla_category is null and is_closed then 1 else 0 end) as closed_without_valid_sla

    from base_data
    group by created_date_key
),

-- ============================================
-- CLOSED COMPLAINTS (by closed date)
-- ============================================
closed_metrics as (
    select
        closed_date_key as date_key,
        count(*) as closed_complaints
    from base_data
    where is_closed  -- ✅ Sadece is_closed check (closed_date_key null olabilir)
    group by closed_date_key
),

-- ============================================
-- TOP COMPLAINT TYPES (Window function)
-- ============================================
complaint_rankings as (
    select
        created_date_key as date_key,
        complaint_type,
        count(*) as complaint_count,
        row_number() over (
            partition by created_date_key
            order by count(*) desc, complaint_type
        ) as rank_num
    from base_data
    where complaint_type is not null
    group by created_date_key, complaint_type
),

top_complaints as (
    select
        date_key,
        max(case when rank_num = 1 then complaint_type end) as top_complaint_type_1,
        max(case when rank_num = 1 then complaint_count end) as top_complaint_type_1_count,
        max(case when rank_num = 2 then complaint_type end) as top_complaint_type_2,
        max(case when rank_num = 2 then complaint_count end) as top_complaint_type_2_count,
        max(case when rank_num = 3 then complaint_type end) as top_complaint_type_3,
        max(case when rank_num = 3 then complaint_count end) as top_complaint_type_3_count
    from complaint_rankings
    where rank_num <= 3
    group by date_key
),

-- ============================================
-- TOP AGENCIES (Window function)
-- ============================================
agency_rankings as (
    select
        created_date_key as date_key,
        agency_code,
        count(*) as agency_count,
        row_number() over (
            partition by created_date_key
            order by count(*) desc, agency_code
        ) as rank_num
    from base_data
    where agency_code is not null
    group by created_date_key, agency_code
),

top_agencies as (
    select
        date_key,
        max(case when rank_num = 1 then agency_code end) as top_agency_1,
        max(case when rank_num = 1 then agency_count end) as top_agency_1_count,
        max(case when rank_num = 2 then agency_code end) as top_agency_2,
        max(case when rank_num = 2 then agency_count end) as top_agency_2_count,
        max(case when rank_num = 3 then agency_code end) as top_agency_3,
        max(case when rank_num = 3 then agency_count end) as top_agency_3_count
    from agency_rankings
    where rank_num <= 3
    group by date_key
),

-- ============================================
-- ROLLING AVERAGES (7-day & 30-day)
-- ============================================
rolling_metrics as (
    select
        date_key,
        total_complaints,
        avg(total_complaints) over (
            order by date_key
            rows between 6 preceding and current row
        ) as complaints_7day_avg,
        avg(total_complaints) over (
            order by date_key
            rows between 29 preceding and current row
        ) as complaints_30day_avg,
        lag(total_complaints, 1) over (order by date_key) as complaints_prev_day,
        lag(total_complaints, 7) over (order by date_key) as complaints_prev_week
    from daily_metrics
),

-- ============================================
-- FINAL ASSEMBLY
-- ============================================
final as (
    select
        -- Date
        dm.date_key,
        dm.date_actual,

        -- Complaint volumes
        dm.total_complaints,
        dm.total_complaints as new_complaints,
        dm.completed_count,

        -- Status breakdown
        dm.new_count,
        dm.in_progress_count,
        dm.waiting_count,
        dm.other_status_count,
        dm.valid_response_count,

        -- Calculated rates
        dm.within_sla_count,
        dm.total_valid_closed_for_sla,

        -- ✅ Response metrics (ONLY from valid data)
        dm.closed_without_valid_time,
        dm.closed_without_valid_sla,
        tc.top_complaint_type_1,
        tc.top_complaint_type_1_count,
        tc.top_complaint_type_2,
        tc.top_complaint_type_2_count,

        -- ✅ Data quality metrics
        tc.top_complaint_type_3,
        tc.top_complaint_type_3_count,
        ta.top_agency_1,

        -- Top complaint types
        ta.top_agency_1_count,
        ta.top_agency_2,
        ta.top_agency_2_count,
        ta.top_agency_3,
        ta.top_agency_3_count,
        coalesce(cm.closed_complaints, 0) as closed_complaints,

        -- Top agencies
        round(dm.completed_count * 100.0 / nullif(dm.total_complaints, 0), 2) as completion_rate,
        round(dm.in_progress_count * 100.0 / nullif(dm.total_complaints, 0), 2) as in_progress_rate,
        round(dm.avg_response_time_hours, 2) as avg_response_time_hours,
        round(dm.median_response_time_hours, 2) as median_response_time_hours,
        round(dm.within_sla_count * 100.0 / nullif(dm.total_valid_closed_for_sla, 0), 2) as sla_compliance_rate,
        round(dm.closed_without_valid_time * 100.0 / nullif(dm.completed_count, 0), 2) as pct_closed_missing_valid_time,

        -- Rolling metrics & trends
        round(rm.complaints_7day_avg, 2) as complaints_7day_avg,
        round(rm.complaints_30day_avg, 2) as complaints_30day_avg,
        case
            when rm.complaints_prev_day is null then null
            else round((dm.total_complaints - rm.complaints_prev_day) * 100.0 / nullif(rm.complaints_prev_day, 0), 2)
        end as day_over_day_change_pct,
        case
            when rm.complaints_prev_week is null then null
            else round((dm.total_complaints - rm.complaints_prev_week) * 100.0 / nullif(rm.complaints_prev_week, 0), 2)
        end as week_over_week_change_pct,

        -- Trend indicator
        case
            when rm.complaints_prev_day is null then 'NO_DATA'
            when dm.total_complaints > rm.complaints_prev_day * 1.1 then 'UP'
            when dm.total_complaints < rm.complaints_prev_day * 0.9 then 'DOWN'
            else 'STABLE'
        end as trend_direction,

        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from daily_metrics dm
    left join closed_metrics cm on dm.date_key = cm.date_key
    left join top_complaints tc on dm.date_key = tc.date_key
    left join top_agencies ta on dm.date_key = ta.date_key
    left join rolling_metrics rm on dm.date_key = rm.date_key
)

select * from final
order by date_key desc
