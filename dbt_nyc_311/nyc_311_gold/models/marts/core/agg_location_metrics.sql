-- Location Metrics Aggregation Table
{{ 
  config(
    materialized='incremental',
    unique_key=['location_key', 'date_key'],
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

-- Base data with location details
location_daily as (
    select
        f.created_date_key as date_key,
        f.location_key,
        l.borough,
        l.city,
        l.zip_code,
        
        -- Complaint counts
        count(*) as total_complaints,
        count(case when f.status = 'OPEN' then 1 end) as open_complaints,
        count(case when f.status = 'CLOSED' then 1 end) as closed_complaints,
        count(*) as new_complaints, -- all created on this date
        
        -- Response metrics
        avg(case when f.status = 'CLOSED' then f.response_time_hours end) as avg_response_time_hours,
        percentile_approx(case when f.status = 'CLOSED' then f.response_time_hours end, 0.5) as median_response_time_hours,
        
        -- SLA metrics
        count(case when f.response_sla_category = 'WITHIN_SLA' and f.status = 'CLOSED' then 1 end) as within_sla_count,
        count(case when f.status = 'CLOSED' then 1 end) as total_closed_for_sla
        
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_location') }} l on f.location_key = l.location_key
    
    {% if is_incremental() %}
    cross join lookback
    where f.created_date_key >= lookback.lookback_date_key
    {% endif %}
    
    group by 1, 2, 3, 4, 5
),

-- Top complaint types per location per day
complaint_type_counts as (
    select 
        f.created_date_key as date_key,
        f.location_key,
        ct.complaint_type,
        count(*) as complaint_count
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_complaint_type') }} ct on f.complaint_type_key = ct.complaint_type_key
    
    {% if is_incremental() %}
    cross join lookback
    where f.created_date_key >= lookback.lookback_date_key
    {% endif %}
    
    group by 1, 2, 3
),

top_complaints as (
    select 
        date_key,
        location_key,
        complaint_type,
        complaint_count,
        row_number() over (partition by date_key, location_key order by complaint_count desc) as rn
    from complaint_type_counts
),

-- 7-day rolling average
rolling_avg as (
    select
        date_key,
        location_key,
        avg(total_complaints) over (
            partition by location_key 
            order by date_key 
            rows between 6 preceding and current row
        ) as complaints_7day_avg
    from location_daily
),

-- Previous week comparison
prev_week as (
    select
        date_key + 7 as compare_date_key,
        location_key,
        total_complaints as prev_week_complaints
    from location_daily
),

-- Borough daily average for hotspot detection
borough_avg as (
    select
        date_key,
        borough,
        avg(total_complaints) as borough_daily_avg
    from location_daily
    group by 1, 2
),

-- Rankings
location_rankings as (
    select
        date_key,
        location_key,
        borough,
        city,
        total_complaints,
        row_number() over (partition by date_key, borough order by total_complaints desc) as borough_rank_daily,
        row_number() over (partition by date_key, borough, city order by total_complaints desc) as city_rank_in_borough
    from location_daily
),

final as (
    select
        -- Keys & Location Info
        ld.date_key,
        ld.location_key,
        ld.borough,
        ld.city,
        ld.zip_code,
        
        -- Volume metrics
        ld.total_complaints,
        ld.open_complaints,
        ld.closed_complaints,
        ld.new_complaints,
        
        -- Performance metrics
        round(ld.avg_response_time_hours, 2) as avg_response_time_hours,
        round(ld.median_response_time_hours, 2) as median_response_time_hours,
        round(ld.within_sla_count * 100.0 / nullif(ld.total_closed_for_sla, 0), 2) as sla_compliance_rate,
        round(ld.closed_complaints * 100.0 / nullif(ld.total_complaints, 0), 2) as completion_rate,
        
        -- Top 3 complaint types
        tc1.complaint_type as top_complaint_type_1,
        tc1.complaint_count as top_complaint_type_1_count,
        tc2.complaint_type as top_complaint_type_2,
        tc2.complaint_count as top_complaint_type_2_count,
        tc3.complaint_type as top_complaint_type_3,
        tc3.complaint_count as top_complaint_type_3_count,
        
        -- Trend metrics
        round(ra.complaints_7day_avg, 2) as complaints_7day_avg,
        round((ld.total_complaints - coalesce(pw.prev_week_complaints, 0)) * 100.0 / 
            nullif(coalesce(pw.prev_week_complaints, 0), 0), 2) as complaints_vs_prev_week,
        case 
            when ld.total_complaints > ba.borough_daily_avg * 1.5 then true 
            else false 
        end as is_hotspot,
        
        -- Rankings
        lr.borough_rank_daily,
        lr.city_rank_in_borough,
        
        -- Context metrics
        ba.borough_daily_avg as borough_avg_complaints,
        round(ld.total_complaints * 100.0 / nullif(ba.borough_daily_avg, 0), 2) as pct_of_borough_avg,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from location_daily ld
    left join rolling_avg ra 
        on ld.date_key = ra.date_key 
        and ld.location_key = ra.location_key
    left join prev_week pw 
        on ld.date_key = pw.compare_date_key 
        and ld.location_key = pw.location_key
    left join borough_avg ba 
        on ld.date_key = ba.date_key 
        and ld.borough = ba.borough
    left join location_rankings lr 
        on ld.date_key = lr.date_key 
        and ld.location_key = lr.location_key
    left join top_complaints tc1 
        on ld.date_key = tc1.date_key 
        and ld.location_key = tc1.location_key 
        and tc1.rn = 1
    left join top_complaints tc2 
        on ld.date_key = tc2.date_key 
        and ld.location_key = tc2.location_key 
        and tc2.rn = 2
    left join top_complaints tc3 
        on ld.date_key = tc3.date_key 
        and ld.location_key = tc3.location_key 
        and tc3.rn = 3
)

select * from final