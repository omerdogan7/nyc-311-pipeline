-- Agency Metrics Aggregation Table - OPTIMIZED FOR WATERMARK-BASED FACT
{{ 
  config(
    materialized='incremental',
    unique_key=['agency_key', 'date_key'],
    on_schema_change='fail',
    tags=['aggregation', 'daily', 'agency']
  ) 
}}

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
        f.agency_key,
        f.status,
        f.is_closed,
        
        -- Response time (only if valid)
        case 
            when f.is_closed 
                and not f.has_invalid_closed_date
                and not f.has_historical_closed_date
                and not f.has_future_closed_date
                and f.response_time_hours is not null
                and f.response_time_hours >= 0
            then f.response_time_hours
            else null
        end as valid_response_time_hours,
        
        -- SLA category (only valid ones)
        case
            when f.response_sla_category in ('INVALID_DATE', 'NEGATIVE_TIME', 'NO_DATE') then null
            else f.response_sla_category
        end as valid_sla_category,
        
        f.has_resolution,
        f.agency_code,
        ct.complaint_category,
        d.date_actual
        
    from {{ ref('fact_311') }} f
    left join {{ ref('dim_complaint_type') }} ct 
        on f.complaint_type_key = ct.complaint_type_key
    inner join {{ ref('dim_date') }} d 
        on f.created_date_key = d.date_key
    
    {% if is_incremental() %}
    -- Use created_date_key for partition pruning
    where f.created_date_key >= {{ min_date_key }}
    {% endif %}
),

-- ============================================
-- AGENCY ATTRIBUTES: Get from dim_agency
-- ============================================
agency_attrs as (
    select
        agency_id as agency_key,
        agency_name,
        agency_category
    from {{ ref('dim_agency') }}
),

-- ============================================
-- MAIN AGGREGATION: All agency metrics
-- ============================================
agency_daily as (
    select
        date_key,
        agency_key,
        max(date_actual) as date_actual,
        
        -- Volume metrics
        count(*) as total_complaints,
        count(*) as new_complaints,
        sum(case when is_closed then 1 else 0 end) as closed_complaints,
        sum(case when status in ('ASSIGNED', 'STARTED', 'IN PROGRESS') then 1 else 0 end) as in_progress_complaints,
        sum(case when status = 'PENDING' then 1 else 0 end) as pending_complaints,
        
        -- Response metrics (only VALID closed cases)
        avg(valid_response_time_hours) as avg_response_time_hours,
        percentile_approx(valid_response_time_hours, 0.5) as median_response_time_hours,
        count(valid_response_time_hours) as valid_response_count,
        
        -- SLA metrics (only VALID data)
        sum(case when valid_sla_category in ('SAME_DAY', 'WITHIN_WEEK') then 1 else 0 end) as within_sla_count,
        count(valid_sla_category) as total_valid_closed_for_sla,
        
        -- Resolution metrics
        sum(case when is_closed and valid_response_time_hours <= 24 then 1 else 0 end) as same_day_resolutions,
        sum(case when has_resolution and valid_response_time_hours <= 48 then 1 else 0 end) as first_touch_resolutions,
        
        -- Data quality tracking
        sum(case when is_closed and valid_response_time_hours is null then 1 else 0 end) as closed_without_valid_time,
        sum(case when valid_sla_category is null and is_closed then 1 else 0 end) as closed_without_valid_sla
        
    from base_data
    group by date_key, agency_key
),

-- ============================================
-- DAILY TOTALS (for percentage calculations)
-- ============================================
daily_totals as (
    select
        date_key,
        count(*) as total_daily_complaints
    from base_data
    group by date_key
),

-- ============================================
-- TOP COMPLAINT CATEGORIES (Window function)
-- ============================================
category_rankings as (
    select
        date_key,
        agency_key,
        complaint_category,
        count(*) as category_count,
        row_number() over (
            partition by date_key, agency_key 
            order by count(*) desc, complaint_category
        ) as rn
    from base_data
    where complaint_category is not null
    group by date_key, agency_key, complaint_category
),

top_categories as (
    select
        date_key,
        agency_key,
        max(case when rn = 1 then complaint_category end) as top_complaint_category_1,
        max(case when rn = 1 then category_count end) as top_complaint_category_1_count,
        max(case when rn = 2 then complaint_category end) as top_complaint_category_2,
        max(case when rn = 2 then category_count end) as top_complaint_category_2_count,
        max(case when rn = 3 then complaint_category end) as top_complaint_category_3,
        max(case when rn = 3 then category_count end) as top_complaint_category_3_count
    from category_rankings
    where rn <= 3
    group by date_key, agency_key
),

-- ============================================
-- ROLLING METRICS (7-day average)
-- ============================================
rolling_metrics as (
    select
        date_key,
        agency_key,
        avg(total_complaints) over (
            partition by agency_key 
            order by date_key 
            rows between 6 preceding and current row
        ) as complaints_7day_avg,
        lag(total_complaints, 7) over (
            partition by agency_key 
            order by date_key
        ) as complaints_prev_week
    from agency_daily
),

-- ============================================
-- AGENCY RANKINGS (by volume and SLA)
-- ============================================
agency_rankings as (
    select
        date_key,
        agency_key,
        row_number() over (
            partition by date_key 
            order by total_complaints desc
        ) as agency_rank_by_volume,
        row_number() over (
            partition by date_key 
            order by 
                case 
                    when total_valid_closed_for_sla > 0 
                    then within_sla_count * 1.0 / total_valid_closed_for_sla 
                    else 0 
                end desc
        ) as agency_rank_by_sla
    from agency_daily
),

-- ============================================
-- OPEN BACKLOG (current state per agency)
-- ============================================
agency_backlog as (
    select
        f.agency_key,
        cast(date_format(current_date(), 'yyyyMMdd') as int) as date_key,
        count(*) as backlog_count,
        avg(datediff(current_date(), d.date_actual)) as avg_open_days
    from {{ ref('fact_311') }} f
    inner join {{ ref('dim_date') }} d 
        on f.created_date_key = d.date_key
    where not f.is_closed  -- Use is_closed flag (consistent with fact)
    group by f.agency_key
),

-- ============================================
-- FINAL ASSEMBLY
-- ============================================
final as (
    select
        -- Keys & Agency Info
        ad.date_key,
        ad.agency_key,
        aa.agency_name,
        aa.agency_category,
        
        -- Volume metrics
        ad.total_complaints,
        ad.new_complaints,
        ad.closed_complaints,
        ad.in_progress_complaints,
        ad.pending_complaints,
        
        -- Performance metrics (ONLY from valid data)
        round(ad.avg_response_time_hours, 2) as avg_response_time_hours,
        round(ad.median_response_time_hours, 2) as median_response_time_hours,
        ad.valid_response_count,
        round(ad.within_sla_count * 100.0 / nullif(ad.total_valid_closed_for_sla, 0), 2) as sla_compliance_rate,
        ad.total_valid_closed_for_sla,
        round(ad.same_day_resolutions * 100.0 / nullif(ad.closed_complaints, 0), 2) as same_day_resolution_rate,
        round(ad.first_touch_resolutions * 100.0 / nullif(ad.total_complaints, 0), 2) as first_touch_resolution_rate,
        
        -- Data quality metrics
        ad.closed_without_valid_time,
        ad.closed_without_valid_sla,
        round(ad.closed_without_valid_time * 100.0 / nullif(ad.closed_complaints, 0), 2) as pct_closed_missing_valid_time,
        
        -- Workload distribution
        round(ad.total_complaints * 100.0 / nullif(dt.total_daily_complaints, 0), 2) as pct_of_total_daily_complaints,
        round(ad.total_complaints / nullif(rm.complaints_7day_avg, 0), 2) as workload_index,
        
        -- Top 3 complaint categories
        tc.top_complaint_category_1,
        tc.top_complaint_category_1_count,
        tc.top_complaint_category_2,
        tc.top_complaint_category_2_count,
        tc.top_complaint_category_3,
        tc.top_complaint_category_3_count,
        
        -- Rankings & Trends
        ar.agency_rank_by_volume,
        ar.agency_rank_by_sla,
        round(rm.complaints_7day_avg, 2) as complaints_7day_avg,
        case
            when rm.complaints_prev_week is null then 'NO_DATA'
            when ad.total_complaints > rm.complaints_prev_week * 1.1 then 'UP'
            when ad.total_complaints < rm.complaints_prev_week * 0.9 then 'DOWN'
            else 'STABLE'
        end as trend_direction,
        
        -- Efficiency metrics
        coalesce(ab.avg_open_days, 0) as avg_open_days,
        coalesce(ab.backlog_count, 0) as backlog_count,
        round(
            (ad.closed_complaints * 100.0 / nullif(ad.total_complaints, 0)) * 
            (ad.within_sla_count * 100.0 / nullif(ad.total_valid_closed_for_sla, 0)) / 100.0
        , 2) as productivity_score,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from agency_daily ad
    inner join agency_attrs aa 
        on ad.agency_key = aa.agency_key
    inner join daily_totals dt 
        on ad.date_key = dt.date_key
    left join rolling_metrics rm 
        on ad.date_key = rm.date_key 
        and ad.agency_key = rm.agency_key
    left join agency_backlog ab 
        on ad.date_key = ab.date_key 
        and ad.agency_key = ab.agency_key
    left join agency_rankings ar 
        on ad.date_key = ar.date_key 
        and ad.agency_key = ar.agency_key
    left join top_categories tc 
        on ad.date_key = tc.date_key 
        and ad.agency_key = tc.agency_key
)

select * from final
order by date_key desc, total_complaints desc

