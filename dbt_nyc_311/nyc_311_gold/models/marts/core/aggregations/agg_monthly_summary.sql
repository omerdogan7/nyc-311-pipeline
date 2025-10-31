-- Monthly Summary Aggregation Table - OPTIMIZED FOR WATERMARK-BASED FACT
{{ config(
    materialized='incremental',
    unique_key='month_key',
    incremental_strategy='merge',
    on_schema_change='fail',
    tags=['aggregation', 'monthly', 'executive']
) }}

{% if is_incremental() %}
    -- ✅ 3 aylık lookback (son 3 ay + 2 ay için rolling average buffer)
    {% set lookback_months = var('incremental_lookback_months', 3) + 2 %}
    
    {% set min_month_key_query %}
        select max(month_key) - {{ lookback_months }}
        from {{ this }}
    {% endset %}
    {% set min_month_key = run_query(min_month_key_query).columns[0][0] %}
{% endif %}

-- ============================================
-- SINGLE BASE SCAN: Get all data in one pass WITH FLAGS
-- ============================================
with base_data as (
    select
        d.year,
        d.month,
        d.month_name,
        cast(concat(d.year, lpad(cast(d.month as string), 2, '0')) as int) as month_key,
        d.date_actual,
        f.status,
        f.is_closed,
        
        -- ✅ Response time (only if valid)
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
        
        -- ✅ SLA category (only valid ones)
        case
            when f.response_sla_category in ('INVALID_DATE', 'NEGATIVE_TIME', 'NO_DATE') then null
            else f.response_sla_category
        end as valid_sla_category,
        
        f.agency_code,
        ct.complaint_category
        
    from {{ ref('fact_311') }} f
    inner join {{ ref('dim_date') }} d 
        on f.created_date_key = d.date_key
    left join {{ ref('dim_complaint_type') }} ct 
        on f.complaint_type_key = ct.complaint_type_key
    
    {% if is_incremental() %}
    -- ✅ Partition pruning için created_year filter (monthly aggregation için yeterli)
    where f.created_year >= year(add_months(current_date(), -{{ lookback_months }}))
    {% endif %}
),

-- ============================================
-- AGENCY ATTRIBUTES: Get from dim_agency
-- ============================================
agency_attrs as (
    select
        agency_key as agency_key,
        agency_name
    from {{ ref('dim_agency') }}
),

-- ============================================
-- MAIN MONTHLY AGGREGATION
-- ============================================
monthly_base as (
    select
        year,
        month,
        max(month_name) as month_name,
        month_key,
        min(date_actual) as month_start_date,
        max(date_actual) as month_end_date,
        
        -- Complaint counts
        count(*) as total_complaints,
        sum(case when is_closed then 1 else 0 end) as completed_complaints,
        sum(case when status = 'OPEN' then 1 else 0 end) as open_complaints,
        sum(case when status in ('ASSIGNED', 'STARTED', 'IN PROGRESS') then 1 else 0 end) as in_progress_complaints,
        sum(case when status = 'PENDING' then 1 else 0 end) as pending_complaints,
        
        -- ✅ Response metrics (only VALID data)
        avg(valid_response_time_hours) as avg_response_time_hours,
        percentile_approx(valid_response_time_hours, 0.5) as median_response_time_hours,
        count(valid_response_time_hours) as valid_response_count,
        
        -- ✅ SLA metrics (only VALID data)
        sum(case when valid_sla_category in ('SAME_DAY', 'WITHIN_WEEK') then 1 else 0 end) as within_sla_count,
        count(valid_sla_category) as total_valid_closed_for_sla,
        
        -- ✅ Data quality tracking
        sum(case when is_closed and valid_response_time_hours is null then 1 else 0 end) as closed_without_valid_time,
        
        -- Business days in month
        count(distinct date_actual) as days_in_month
        
    from base_data
    group by year, month, month_key
),

-- ============================================
-- PREVIOUS MONTH COMPARISON
-- ============================================
previous_month as (
    select
        month_key + 1 as comparison_month_key,
        total_complaints as total_complaints_prev_month,
        completed_complaints as completed_complaints_prev_month,
        valid_response_count as valid_response_count_prev_month
    from monthly_base
),

-- ============================================
-- PREVIOUS YEAR COMPARISON (same month)
-- ============================================
previous_year as (
    select
        year + 1 as comparison_year,
        month,
        count(*) as total_complaints_prev_year,
        sum(case when is_closed then 1 else 0 end) as completed_complaints_prev_year,
        count(valid_response_time_hours) as valid_response_count_prev_year
    from base_data
    group by year, month
),

-- ============================================
-- TOP CATEGORIES (Window function)
-- ============================================
category_rankings as (
    select
        month_key,
        complaint_category,
        count(*) as category_count,
        row_number() over (
            partition by month_key 
            order by count(*) desc, complaint_category
        ) as rn
    from base_data
    where complaint_category is not null
    group by month_key, complaint_category
),

top_categories as (
    select
        month_key,
        max(case when rn = 1 then complaint_category end) as top_complaint_category_1,
        max(case when rn = 1 then category_count end) as top_complaint_category_1_count,
        max(case when rn = 2 then complaint_category end) as top_complaint_category_2,
        max(case when rn = 2 then category_count end) as top_complaint_category_2_count,
        max(case when rn = 3 then complaint_category end) as top_complaint_category_3,
        max(case when rn = 3 then category_count end) as top_complaint_category_3_count,
        max(case when rn = 4 then complaint_category end) as top_complaint_category_4,
        max(case when rn = 4 then category_count end) as top_complaint_category_4_count,
        max(case when rn = 5 then complaint_category end) as top_complaint_category_5,
        max(case when rn = 5 then category_count end) as top_complaint_category_5_count
    from category_rankings
    where rn <= 5
    group by month_key
),

-- ============================================
-- TOP AGENCIES (Window function)
-- ============================================
agency_rankings as (
    select
        month_key,
        agency_code,
        count(*) as agency_count,
        row_number() over (
            partition by month_key 
            order by count(*) desc, agency_code
        ) as rn
    from base_data
    where agency_code is not null
    group by month_key, agency_code
),

top_agencies as (
    select
        month_key,
        max(case when rn = 1 then agency_code end) as top_agency_1_code,
        max(case when rn = 1 then agency_count end) as top_agency_1_count,
        max(case when rn = 2 then agency_code end) as top_agency_2_code,
        max(case when rn = 2 then agency_count end) as top_agency_2_count,
        max(case when rn = 3 then agency_code end) as top_agency_3_code,
        max(case when rn = 3 then agency_count end) as top_agency_3_count
    from agency_rankings
    where rn <= 3
    group by month_key
),

-- ============================================
-- END OF MONTH OPEN COMPLAINTS
-- ============================================
month_end_dates as (
    select
        month_key,
        max(date_actual) as max_date
    from base_data
    group by month_key
),

open_eom as (
    select
        bd.month_key,
        count(*) as open_complaints_eom
    from base_data bd
    inner join month_end_dates med
        on bd.month_key = med.month_key
        and bd.date_actual = med.max_date
    where bd.status = 'OPEN'
    group by bd.month_key
),

-- ============================================
-- ROLLING 3-MONTH AVERAGE
-- ============================================
rolling_metrics as (
    select
        month_key,
        avg(total_complaints) over (
            order by month_key 
            rows between 2 preceding and current row
        ) as complaints_3month_avg,
        avg(completed_complaints) over (
            order by month_key 
            rows between 2 preceding and current row
        ) as completed_3month_avg,
        avg(valid_response_count) over (
            order by month_key 
            rows between 2 preceding and current row
        ) as valid_responses_3month_avg
    from monthly_base
),

-- ============================================
-- FINAL ASSEMBLY
-- ============================================
final as (
    select
        -- Date fields
        mb.month_key,
        mb.year,
        mb.month,
        mb.month_name,
        mb.month_start_date,
        mb.month_end_date,
        mb.days_in_month,
        
        -- Volume metrics
        mb.total_complaints,
        round(mb.total_complaints * 1.0 / mb.days_in_month, 2) as avg_daily_complaints,
        mb.completed_complaints,
        mb.open_complaints,
        mb.in_progress_complaints,
        mb.pending_complaints,
        coalesce(oe.open_complaints_eom, mb.open_complaints) as open_complaints_eom,
        
        -- ✅ Performance metrics (ONLY from valid data)
        round(mb.avg_response_time_hours, 2) as avg_response_time_hours,
        round(mb.median_response_time_hours, 2) as median_response_time_hours,
        mb.valid_response_count,
        round(mb.within_sla_count * 100.0 / nullif(mb.total_valid_closed_for_sla, 0), 2) as sla_compliance_rate,
        mb.total_valid_closed_for_sla,
        round(mb.completed_complaints * 100.0 / nullif(mb.total_complaints, 0), 2) as completion_rate,
        
        -- ✅ Data quality metrics
        mb.closed_without_valid_time,
        round(mb.closed_without_valid_time * 100.0 / nullif(mb.completed_complaints, 0), 2) as pct_closed_missing_valid_time,
        
        -- Derived metrics
        round(mb.total_complaints * 1.0 / nullif(mb.completed_complaints, 0), 2) as completion_ratio,
        round(mb.open_complaints * 100.0 / nullif(mb.total_complaints, 0), 2) as open_rate,
        round(mb.in_progress_complaints * 100.0 / nullif(mb.total_complaints, 0), 2) as in_progress_rate,
        
        -- MoM comparison
        coalesce(pm.total_complaints_prev_month, 0) as total_complaints_prev_month,
        round((mb.total_complaints - coalesce(pm.total_complaints_prev_month, 0)) * 100.0 / 
            nullif(pm.total_complaints_prev_month, 0), 2) as mom_growth_rate,
        mb.total_complaints - coalesce(pm.total_complaints_prev_month, 0) as mom_growth_absolute,
        
        -- MoM data quality comparison
        coalesce(pm.valid_response_count_prev_month, 0) as valid_response_count_prev_month,
        round((mb.valid_response_count - coalesce(pm.valid_response_count_prev_month, 0)) * 100.0 / 
            nullif(pm.valid_response_count_prev_month, 0), 2) as mom_valid_data_growth_rate,
        
        -- YoY comparison
        coalesce(py.total_complaints_prev_year, 0) as total_complaints_prev_year,
        round((mb.total_complaints - coalesce(py.total_complaints_prev_year, 0)) * 100.0 / 
            nullif(py.total_complaints_prev_year, 0), 2) as yoy_growth_rate,
        mb.total_complaints - coalesce(py.total_complaints_prev_year, 0) as yoy_growth_absolute,
        case
            when py.total_complaints_prev_year is null then 'NO_DATA'
            when mb.total_complaints > py.total_complaints_prev_year * 1.1 then 'GROWTH'
            when mb.total_complaints < py.total_complaints_prev_year * 0.9 then 'DECLINE'
            else 'STABLE'
        end as yoy_trend,
        
        -- YoY data quality comparison
        coalesce(py.valid_response_count_prev_year, 0) as valid_response_count_prev_year,
        round((mb.valid_response_count - coalesce(py.valid_response_count_prev_year, 0)) * 100.0 / 
            nullif(py.valid_response_count_prev_year, 0), 2) as yoy_valid_data_growth_rate,
        
        -- Rolling averages
        round(rm.complaints_3month_avg, 2) as complaints_3month_avg,
        round(rm.completed_3month_avg, 2) as completed_3month_avg,
        round(rm.valid_responses_3month_avg, 2) as valid_responses_3month_avg,
        
        -- Top 5 complaint categories
        tc.top_complaint_category_1,
        tc.top_complaint_category_1_count,
        tc.top_complaint_category_2,
        tc.top_complaint_category_2_count,
        tc.top_complaint_category_3,
        tc.top_complaint_category_3_count,
        tc.top_complaint_category_4,
        tc.top_complaint_category_4_count,
        tc.top_complaint_category_5,
        tc.top_complaint_category_5_count,
        
        -- Top 3 agencies (code + name from dim_agency)
        ta.top_agency_1_code,
        aa1.agency_name as top_agency_1_name,
        ta.top_agency_1_count,
        ta.top_agency_2_code,
        aa2.agency_name as top_agency_2_name,
        ta.top_agency_2_count,
        ta.top_agency_3_code,
        aa3.agency_name as top_agency_3_name,
        ta.top_agency_3_count,
        
        -- Category concentration (top 3 as % of total)
        round(
            (coalesce(tc.top_complaint_category_1_count, 0) + 
             coalesce(tc.top_complaint_category_2_count, 0) + 
             coalesce(tc.top_complaint_category_3_count, 0)) * 100.0 / 
            nullif(mb.total_complaints, 0), 2
        ) as top_3_categories_pct,
        
        -- Agency concentration (top 3 as % of total)
        round(
            (coalesce(ta.top_agency_1_count, 0) + 
             coalesce(ta.top_agency_2_count, 0) + 
             coalesce(ta.top_agency_3_count, 0)) * 100.0 / 
            nullif(mb.total_complaints, 0), 2
        ) as top_3_agencies_pct,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from monthly_base mb
    left join previous_month pm 
        on mb.month_key = pm.comparison_month_key
    left join previous_year py 
        on mb.year = py.comparison_year 
        and mb.month = py.month
    left join open_eom oe 
        on mb.month_key = oe.month_key
    left join top_categories tc 
        on mb.month_key = tc.month_key
    left join top_agencies ta 
        on mb.month_key = ta.month_key
    left join rolling_metrics rm 
        on mb.month_key = rm.month_key
    left join agency_attrs aa1 
        on ta.top_agency_1_code = aa1.agency_key
    left join agency_attrs aa2 
        on ta.top_agency_2_code = aa2.agency_key
    left join agency_attrs aa3 
        on ta.top_agency_3_code = aa3.agency_key
)

select * from final
order by month_key desc

-- ========================================
-- VALIDATION QUERIES
-- ========================================
/*
-- 1. Monthly trend with data quality (last 12 months)
SELECT 
    month_key,
    year,
    month_name,
    total_complaints,
    completed_complaints,
    valid_response_count,
    ROUND(valid_response_count * 100.0 / NULLIF(completed_complaints, 0), 2) as valid_data_pct,
    avg_daily_complaints,
    completion_rate,
    sla_compliance_rate,
    pct_closed_missing_valid_time,
    yoy_growth_rate,
    yoy_trend
FROM {{ this }}
WHERE month_key >= CAST(date_format(add_months(current_date(), -12), 'yyyyMM') AS int)
ORDER BY month_key DESC;

-- 2. YoY comparison with data quality
SELECT 
    year,
    SUM(total_complaints) as annual_total,
    SUM(completed_complaints) as annual_completed,
    SUM(valid_response_count) as annual_valid,
    ROUND(SUM(valid_response_count) * 100.0 / NULLIF(SUM(completed_complaints), 0), 2) as valid_pct,
    AVG(sla_compliance_rate) as avg_sla_rate,
    AVG(completion_rate) as avg_completion_rate,
    AVG(avg_response_time_hours) as avg_response_hours
FROM {{ this }}
GROUP BY year
ORDER BY year DESC;

-- 3. Incremental run validation
SELECT 
    MIN(month_key) as earliest_month,
    MAX(month_key) as latest_month,
    COUNT(*) as months_processed,
    MAX(updated_at) as last_updated
FROM {{ this }};
*/

/*
✅ KEY OPTIMIZATIONS:
====================
1. Jinja variable for min_month_key (tek subquery)
2. Extended lookback: 3 + 2 = 5 months (rolling averages + comparisons için)
3. Partition pruning: created_year filter (monthly aggregation için yeterli)
4. Fact table ile tutarlı data quality logic
5. add_months() kullanımı (Databricks best practice)

FACT TABLE ILE UYUMLULUK:
=========================
✅ Watermark-based fact table ile uyumlu
✅ Partition pruning (created_year filter)
✅ is_closed flag kullanımı
✅ Data quality filtering (valid_* fields)
✅ 3 aylık lookback + 2 ay buffer (rolling için)
*/