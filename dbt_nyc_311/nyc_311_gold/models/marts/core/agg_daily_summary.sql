-- Daily Summary Aggregation Table
{{ 
  config(
    materialized='incremental',
    unique_key='date_key',
    on_schema_change='fail',
    tags=['aggregation', 'daily']
  ) 
}}

with daily_complaints as (
    select
        f.created_date_key as date_key,
        d.date_actual,
        
        -- Total count
        count(*) as total_complaints,
        
        -- Status group counts
        count(case when f.status = 'CLOSED' then 1 end) as completed_count,
        count(case when f.status = 'OPEN' then 1 end) as new_count,
        count(case when f.status in ('ASSIGNED', 'STARTED', 'IN PROGRESS') then 1 end) as in_progress_count,
        count(case when f.status = 'PENDING' then 1 end) as waiting_count,
        count(case when f.status in ('CANCEL', 'UNSPECIFIED') then 1 end) as other_status_count,
        
        -- Response metrics (only for closed)
        avg(case when f.status = 'CLOSED' then f.response_time_hours end) as avg_response_time_hours,
        percentile_cont(0.5) within group (
            order by case when f.status = 'CLOSED' then f.response_time_hours end
        ) as median_response_time_hours,
        
        -- SLA metrics
        count(case when f.response_sla_category = 'WITHIN_SLA' and f.status = 'CLOSED' then 1 end) as within_sla_count,
        count(case when f.response_sla_category = 'WITHIN_SLA' and f.status = 'CLOSED' then 1 end) * 100.0 / 
            nullif(count(case when f.status = 'CLOSED' then 1 end), 0) as sla_compliance_rate
        
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_date') }} d on f.created_date_key = d.date_key
    
    {% if is_incremental() %}
        where f.created_date_key >= (
            select max(date_key) - 3 from {{ this }}  -- 3 day lookback for late data
        )
    {% endif %}
    
    group by 1, 2
),

-- Get top complaint types per day
top_complaints as (
    select 
        f.created_date_key as date_key,
        ct.complaint_type,
        count(*) as complaint_count,
        row_number() over (partition by f.created_date_key order by count(*) desc) as rn
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_complaint_type') }} ct on f.complaint_type_key = ct.complaint_type_key
    
    {% if is_incremental() %}
        where f.created_date_key >= (
            select max(date_key) - 3 from {{ this }}
        )
    {% endif %}
    
    group by 1, 2
),

-- Get top agencies per day
top_agencies as (
    select 
        f.created_date_key as date_key,
        a.agency_key as agency,
        count(*) as agency_count,
        row_number() over (partition by f.created_date_key order by count(*) desc) as rn
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_agency') }} a on f.agency_key = a.agency_key
    
    {% if is_incremental() %}
        where f.created_date_key >= (
            select max(date_key) - 3 from {{ this }}
        )
    {% endif %}
    
    group by 1, 2
),

-- Get closed complaints count by closed date
closed_by_date as (
    select 
        closed_date_key as date_key,
        count(*) as closed_complaints
    from {{ ref('fct_complaints') }}
    where closed_date_key is not null
    
    {% if is_incremental() %}
        and closed_date_key >= (
            select max(date_key) - 3 from {{ this }}
        )
    {% endif %}
    
    group by 1
),

final as (
    select
        -- Date
        dc.date_key,
        dc.date_actual,
        
        -- Complaint counts
        dc.total_complaints,
        dc.total_complaints as new_complaints,  -- created on this date
        coalesce(cbd.closed_complaints, 0) as closed_complaints,
        
        -- Status group breakdown
        dc.completed_count,
        dc.new_count,
        dc.in_progress_count,
        dc.waiting_count,
        dc.other_status_count,
        
        -- Calculated percentages
        round(dc.completed_count * 100.0 / nullif(dc.total_complaints, 0), 2) as completion_rate,
        round(dc.in_progress_count * 100.0 / nullif(dc.total_complaints, 0), 2) as in_progress_rate,
        
        -- Response metrics
        round(dc.avg_response_time_hours, 2) as avg_response_time_hours,
        round(dc.median_response_time_hours, 2) as median_response_time_hours,
        dc.within_sla_count,
        round(dc.sla_compliance_rate, 2) as sla_compliance_rate,
        
        -- Top complaint types
        tc1.complaint_type as top_complaint_type_1,
        tc1.complaint_count as top_complaint_type_1_count,
        tc2.complaint_type as top_complaint_type_2,
        tc2.complaint_count as top_complaint_type_2_count,
        tc3.complaint_type as top_complaint_type_3,
        tc3.complaint_count as top_complaint_type_3_count,
        
        -- Top agencies
        ta1.agency as top_agency_1,
        ta1.agency_count as top_agency_1_count,
        ta2.agency as top_agency_2,
        ta2.agency_count as top_agency_2_count,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from daily_complaints dc
    left join closed_by_date cbd on dc.date_key = cbd.date_key
    left join top_complaints tc1 on dc.date_key = tc1.date_key and tc1.rn = 1
    left join top_complaints tc2 on dc.date_key = tc2.date_key and tc2.rn = 2
    left join top_complaints tc3 on dc.date_key = tc3.date_key and tc3.rn = 3
    left join top_agencies ta1 on dc.date_key = ta1.date_key and ta1.rn = 1
    left join top_agencies ta2 on dc.date_key = ta2.date_key and ta2.rn = 2
)

select * from final
order by date_key desc