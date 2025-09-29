-- Monthly Summary Aggregation Table
{{ 
  config(
    materialized='incremental',
    unique_key='month_key',
    on_schema_change='fail',
    tags=['aggregation', 'monthly']
  ) 
}}

-- Get lookback months for incremental runs
{% if is_incremental() %}
with lookback as (
    select 
        cast(date_format(date_add(max(d.date_actual), -90), 'yyyyMM') as int) as lookback_month_key
    from {{ this }} t
    join {{ ref('dim_date') }} d 
        on cast(concat(t.month_key, '01') as int) = d.date_key
),
{% endif %}

-- Base monthly data
monthly_base as (
    select
        d.year,
        d.month,
        d.month_name,
        cast(concat(d.year, lpad(d.month, 2, '0')) as int) as month_key,
        min(d.date_actual) as month_start_date,
        max(d.date_actual) as month_end_date,
        
        -- Complaint counts
        count(*) as total_complaints,
        count(case when f.status = 'CLOSED' then 1 end) as completed_complaints,
        count(case when f.status = 'OPEN' then 1 end) as open_complaints,
        
        -- Response metrics (only for closed)
        avg(case when f.status = 'CLOSED' then f.response_time_hours end) as avg_response_time_hours,
        percentile_approx(case when f.status = 'CLOSED' then f.response_time_hours end, 0.5) as median_response_time_hours,
        
        -- SLA metrics
        count(case when f.response_sla_category = 'WITHIN_SLA' and f.status = 'CLOSED' then 1 end) as within_sla_count,
        count(case when f.status = 'CLOSED' then 1 end) as total_closed_for_sla
        
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_date') }} d on f.created_date_key = d.date_key
    
    {% if is_incremental() %}
    cross join lookback
    where cast(concat(d.year, lpad(d.month, 2, '0')) as int) >= lookback.lookback_month_key
    {% endif %}
    
    group by 1, 2, 3
),

-- Previous year data for YoY comparison
previous_year as (
    select
        d.year + 1 as comparison_year,
        d.month,
        count(*) as total_complaints_prev_year
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_date') }} d on f.created_date_key = d.date_key
    
    {% if is_incremental() %}
    cross join lookback
    where cast(concat(d.year + 1, lpad(d.month, 2, '0')) as int) >= lookback.lookback_month_key
    {% endif %}
    
    group by 1, 2
),

-- Top complaint categories by month
category_counts as (
    select 
        cast(concat(d.year, lpad(d.month, 2, '0')) as int) as month_key,
        ct.complaint_category,
        count(*) as category_count
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_date') }} d on f.created_date_key = d.date_key
    inner join {{ ref('dim_complaint_type') }} ct on f.complaint_type_key = ct.complaint_type_key
    
    {% if is_incremental() %}
    cross join lookback
    where cast(concat(d.year, lpad(d.month, 2, '0')) as int) >= lookback.lookback_month_key
    {% endif %}
    
    group by 1, 2
),

top_categories as (
    select 
        month_key,
        complaint_category,
        category_count,
        row_number() over (partition by month_key order by category_count desc) as rn
    from category_counts
),

-- Top agencies by month
agency_counts as (
    select 
        cast(concat(d.year, lpad(d.month, 2, '0')) as int) as month_key,
        a.agency_key,
        a.agency_name,
        count(*) as agency_count
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_date') }} d on f.created_date_key = d.date_key
    inner join {{ ref('dim_agency') }} a on f.agency_key = a.agency_key
    
    {% if is_incremental() %}
    cross join lookback
    where cast(concat(d.year, lpad(d.month, 2, '0')) as int) >= lookback.lookback_month_key
    {% endif %}
    
    group by 1, 2, 3
),

top_agencies as (
    select 
        month_key,
        agency_key,
        agency_name,
        agency_count,
        row_number() over (partition by month_key order by agency_count desc) as rn
    from agency_counts
),

-- End of month open complaints
open_eom as (
    select 
        cast(concat(d.year, lpad(d.month, 2, '0')) as int) as month_key,
        count(*) as open_complaints_eom
    from {{ ref('fct_complaints') }} f
    inner join {{ ref('dim_date') }} d on f.created_date_key = d.date_key
    where f.status = 'OPEN'
        and d.date_actual = last_day(d.date_actual)  -- Last day of month
    
    {% if is_incremental() %}
    cross join lookback
    where cast(concat(d.year, lpad(d.month, 2, '0')) as int) >= lookback.lookback_month_key
    {% endif %}
    
    group by 1
),

final as (
    select
        -- Date fields
        mb.month_key,
        mb.year,
        mb.month,
        mb.month_name,
        mb.month_start_date,
        mb.month_end_date,
        
        -- Volume metrics
        mb.total_complaints,
        round(mb.total_complaints / day(last_day(mb.month_end_date)), 2) as avg_daily_complaints,
        mb.completed_complaints,
        mb.open_complaints,
        coalesce(oe.open_complaints_eom, mb.open_complaints) as open_complaints_eom,
        
        -- Performance metrics
        round(mb.avg_response_time_hours, 2) as avg_response_time_hours,
        round(mb.median_response_time_hours, 2) as median_response_time_hours,
        round(mb.within_sla_count * 100.0 / nullif(mb.total_closed_for_sla, 0), 2) as sla_compliance_rate,
        round(mb.completed_complaints * 100.0 / nullif(mb.total_complaints, 0), 2) as completion_rate,
        
        -- YoY comparison
        coalesce(py.total_complaints_prev_year, 0) as total_complaints_prev_year,
        round((mb.total_complaints - coalesce(py.total_complaints_prev_year, 0)) * 100.0 / 
            nullif(coalesce(py.total_complaints_prev_year, 0), 0), 2) as yoy_growth_rate,
        mb.total_complaints - coalesce(py.total_complaints_prev_year, 0) as yoy_growth_absolute,
        
        -- Top 5 complaint categories
        tc1.complaint_category as top_complaint_category_1,
        tc1.category_count as top_complaint_category_1_count,
        tc2.complaint_category as top_complaint_category_2,
        tc2.category_count as top_complaint_category_2_count,
        tc3.complaint_category as top_complaint_category_3,
        tc3.category_count as top_complaint_category_3_count,
        tc4.complaint_category as top_complaint_category_4,
        tc4.category_count as top_complaint_category_4_count,
        tc5.complaint_category as top_complaint_category_5,
        tc5.category_count as top_complaint_category_5_count,
        
        -- Top 3 agencies
        ta1.agency_key as top_agency_1,
        ta1.agency_name as top_agency_1_name,
        ta1.agency_count as top_agency_1_count,
        ta2.agency_key as top_agency_2,
        ta2.agency_name as top_agency_2_name,
        ta2.agency_count as top_agency_2_count,
        ta3.agency_key as top_agency_3,
        ta3.agency_name as top_agency_3_name,
        ta3.agency_count as top_agency_3_count,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from monthly_base mb
    left join previous_year py on mb.year = py.comparison_year and mb.month = py.month
    left join open_eom oe on mb.month_key = oe.month_key
    left join top_categories tc1 on mb.month_key = tc1.month_key and tc1.rn = 1
    left join top_categories tc2 on mb.month_key = tc2.month_key and tc2.rn = 2
    left join top_categories tc3 on mb.month_key = tc3.month_key and tc3.rn = 3
    left join top_categories tc4 on mb.month_key = tc4.month_key and tc4.rn = 4
    left join top_categories tc5 on mb.month_key = tc5.month_key and tc5.rn = 5
    left join top_agencies ta1 on mb.month_key = ta1.month_key and ta1.rn = 1
    left join top_agencies ta2 on mb.month_key = ta2.month_key and ta2.rn = 2
    left join top_agencies ta3 on mb.month_key = ta3.month_key and ta3.rn = 3
)

select * from final