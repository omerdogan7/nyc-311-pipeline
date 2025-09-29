-- Agency Metrics Aggregation Table
{{ 
  config(
    materialized='incremental',
    unique_key=['agency_key', 'date_key'],
    on_schema_change='fail',
    tags=['aggregation', 'daily']
  ) 
}}

-- Get lookback date for incremental runs
{% if is_incremental() %}
with lookback as (
    select cast(date_format(date_add(max(d.date_actual), -7), 'yyyyMMdd') as int) as lookback_date_key
    from {{ this }} t
    join {{ ref('dim_date') }} d on t.date_key = d.date_key
),
{% endif %}

-- Base agency daily metrics
agency_daily as (
    select
        f.created_date_key as date_key,
        f.agency_key,
        a.agency_name,
        a.agency_category,
        
        -- Volume metrics
        count(*) as total_complaints,
        count(*) as new_complaints,
        count(case when f.status = 'CLOSED' then 1 end) as closed_complaints,
        count(case when f.status in ('ASSIGNED', 'STARTED', 'IN PROGRESS') then 1 end) as in_progress_complaints,
        count(case when f.status = 'PENDING' then 1 end) as pending_complaints,
        
        -- Response metrics
        avg(case when f.status = 'CLOSED' then f.response_time_hours end) as avg_response_time_hours,
        percentile_approx(case when f.status = 'CLOSED' then f.response_time_hours end, 0.5) as median_response_time_hours,
        
        -- SLA metrics
        count(case when f.response_sla_category = 'WITHIN_SLA' and f.status = 'CLOSED' then 1 end) as within_sla_count,
        count(case when f.status = 'CLOSED' then 1 end) as total_closed_for_sla,
        
        -- Resolution metrics
        count(case when f.status = 'CLOSED' and f.response_time_hours <= 24 then 1 end) as same_day_resolutions,
        count(case when f.has_resolution and f.response_time_hours <= 48 then 1 end) as first_touch_resolutions
        
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_agency') }} a on f.agency_key = a.agency_key
    
    {% if is_incremental() %}
    cross join lookback
    where f.created_date_key >= lookback.lookback_date_key
    {% endif %}
    
    group by 1, 2, 3, 4
),

-- Total daily complaints for percentage calculation
daily_totals as (
    select
        created_date_key as date_key,
        count(*) as total_daily_complaints
    from {{ ref('fct_complaints') }}
    
    {% if is_incremental() %}
    cross join lookback
    where created_date_key >= lookback.lookback_date_key
    {% endif %}
    
    group by 1
),

-- Top complaint categories per agency
category_counts as (
    select 
        f.created_date_key as date_key,
        f.agency_key,
        ct.complaint_category,
        count(*) as category_count
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_complaint_type') }} ct on f.complaint_type_key = ct.complaint_type_key
    
    {% if is_incremental() %}
    cross join lookback
    where f.created_date_key >= lookback.lookback_date_key
    {% endif %}
    
    group by 1, 2, 3
),

top_categories as (
    select 
        date_key,
        agency_key,
        complaint_category,
        category_count,
        row_number() over (partition by date_key, agency_key order by category_count desc) as rn
    from category_counts
),

-- 7-day rolling metrics
rolling_metrics as (
    select
        date_key,
        agency_key,
        avg(total_complaints) over (
            partition by agency_key 
            order by date_key 
            rows between 6 preceding and current row
        ) as complaints_7day_avg,
        lag(total_complaints, 7) over (partition by agency_key order by date_key) as complaints_prev_week
    from agency_daily
),

-- Open complaints and backlog
agency_backlog as (
    select
        f.agency_key,
        cast(date_format(current_date(), 'yyyyMMdd') as int) as date_key,
        count(*) as backlog_count,
        avg(datediff(current_date(), d.date_actual)) as avg_open_days
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_date') }} d on f.created_date_key = d.date_key
    where f.status != 'CLOSED'
    group by 1, 2
),

-- Agency rankings
agency_rankings as (
    select
        ad.date_key,
        ad.agency_key,
        row_number() over (partition by ad.date_key order by ad.total_complaints desc) as agency_rank_by_volume,
        row_number() over (
            partition by ad.date_key 
            order by ad.within_sla_count * 1.0 / nullif(ad.total_closed_for_sla, 0) desc
        ) as agency_rank_by_sla
    from agency_daily ad
),

final as (
    select
        -- Keys & Agency Info
        ad.date_key,
        ad.agency_key,
        ad.agency_name,
        ad.agency_category,
        
        -- Volume metrics
        ad.total_complaints,
        ad.new_complaints,
        ad.closed_complaints,
        ad.in_progress_complaints,
        ad.pending_complaints,
        
        -- Performance metrics
        round(ad.avg_response_time_hours, 2) as avg_response_time_hours,
        round(ad.median_response_time_hours, 2) as median_response_time_hours,
        round(ad.within_sla_count * 100.0 / nullif(ad.total_closed_for_sla, 0), 2) as sla_compliance_rate,
        round(ad.same_day_resolutions * 100.0 / nullif(ad.closed_complaints, 0), 2) as same_day_resolution_rate,
        round(ad.first_touch_resolutions * 100.0 / nullif(ad.total_complaints, 0), 2) as first_touch_resolution_rate,
        
        -- Workload distribution
        round(ad.total_complaints * 100.0 / nullif(dt.total_daily_complaints, 0), 2) as pct_of_total_daily_complaints,
        round(ad.total_complaints / nullif(rm.complaints_7day_avg, 0), 2) as workload_index,
        
        -- Top 3 complaint categories
        tc1.complaint_category as top_complaint_category_1,
        tc1.category_count as top_complaint_category_1_count,
        tc2.complaint_category as top_complaint_category_2,
        tc2.category_count as top_complaint_category_2_count,
        tc3.complaint_category as top_complaint_category_3,
        tc3.category_count as top_complaint_category_3_count,
        
        -- Rankings & Trends
        ar.agency_rank_by_volume,
        ar.agency_rank_by_sla,
        round(rm.complaints_7day_avg, 2) as complaints_7day_avg,
        case
            when ad.total_complaints > rm.complaints_prev_week * 1.1 then 'UP'
            when ad.total_complaints < rm.complaints_prev_week * 0.9 then 'DOWN'
            else 'STABLE'
        end as trend_direction,
        
        -- Efficiency metrics
        coalesce(ab.avg_open_days, 0) as avg_open_days,
        coalesce(ab.backlog_count, 0) as backlog_count,
        round(
            (ad.closed_complaints * 100.0 / nullif(ad.total_complaints, 0)) * 
            (ad.within_sla_count * 100.0 / nullif(ad.total_closed_for_sla, 0)) / 100
        , 2) as productivity_score,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from agency_daily ad
    inner join daily_totals dt on ad.date_key = dt.date_key
    left join rolling_metrics rm on ad.date_key = rm.date_key and ad.agency_key = rm.agency_key
    left join agency_backlog ab on ad.date_key = ab.date_key and ad.agency_key = ab.agency_key
    left join agency_rankings ar on ad.date_key = ar.date_key and ad.agency_key = ar.agency_key
    left join top_categories tc1 on ad.date_key = tc1.date_key and ad.agency_key = tc1.agency_key and tc1.rn = 1
    left join top_categories tc2 on ad.date_key = tc2.date_key and ad.agency_key = tc2.agency_key and tc2.rn = 2
    left join top_categories tc3 on ad.date_key = tc3.date_key and ad.agency_key = tc3.agency_key and tc3.rn = 3
)

select * from final