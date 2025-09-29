-- Holiday Pattern Analysis for NYC 311 Complaints
{{ 
  config(
    materialized='table',
    tags=['analytics', 'holiday']
  ) 
}}

with holiday_periods as (
    select
        date_key,
        date_actual,
        year,
        month,
        day_name,
        is_weekend,
        holiday_name,
        is_holiday,
        
        -- Define holiday window (3 days before and after)
        case 
            when is_holiday then 0
            when lead(is_holiday, 1) over (order by date_actual) then -1
            when lead(is_holiday, 2) over (order by date_actual) then -2
            when lead(is_holiday, 3) over (order by date_actual) then -3
            when lag(is_holiday, 1) over (order by date_actual) then 1
            when lag(is_holiday, 2) over (order by date_actual) then 2
            when lag(is_holiday, 3) over (order by date_actual) then 3
            else null
        end as days_from_holiday,
        
        -- Get the nearest holiday name
        coalesce(
            holiday_name,
            lag(holiday_name, 1) over (order by date_actual),
            lag(holiday_name, 2) over (order by date_actual),
            lag(holiday_name, 3) over (order by date_actual),
            lead(holiday_name, 1) over (order by date_actual),
            lead(holiday_name, 2) over (order by date_actual),
            lead(holiday_name, 3) over (order by date_actual)
        ) as nearest_holiday,
        
        -- Holiday categories
        case
            when holiday_name in ('Christmas Day', 'Thanksgiving', 'New Year\'s Day') then 'Major Holiday'
            when holiday_name in ('Independence Day', 'Memorial Day', 'Labor Day') then 'Summer Holiday'
            when holiday_name in ('Martin Luther King Jr. Day', 'Presidents\' Day', 'Columbus Day', 'Veterans Day') then 'Federal Holiday'
            when holiday_name in ('Juneteenth') then 'Recent Holiday'
            else null
        end as holiday_category
        
    from {{ ref('dim_date') }}
    where year between 2019 and 2023
),

complaint_with_holidays as (
    select
        f.*,
        hp.holiday_name,
        hp.is_holiday,
        hp.days_from_holiday,
        hp.nearest_holiday,
        hp.holiday_category,
        hp.is_weekend,
        hp.year,
        hp.month,
        a.agency_name,
        a.agency_category,
        l.borough,
        ct.complaint_category,
        ct.complaint_type
    from {{ ref('fct_complaints') }} f
    inner join holiday_periods hp on f.created_date_key = hp.date_key
    inner join {{ ref('dim_agency') }} a on f.agency_key = a.agency_key
    inner join {{ ref('dim_location') }} l on f.location_key = l.location_key
    inner join {{ ref('dim_complaint_type') }} ct on f.complaint_type_key = ct.complaint_type_key
    where hp.year between 2019 and 2023
),

-- Daily pattern around holidays
daily_holiday_pattern as (
    select
        nearest_holiday,
        holiday_category,
        days_from_holiday,
        
        -- Volume metrics
        count(*) as complaint_count,
        avg(count(*)) over (partition by nearest_holiday) as avg_holiday_complaints,
        
        -- Category distribution
        count(case when complaint_category = 'Noise' then 1 end) as noise_complaints,
        count(case when complaint_category = 'Transportation' then 1 end) as transportation_complaints,
        count(case when complaint_category = 'Building & Housing' then 1 end) as housing_complaints,
        count(case when complaint_category = 'Sanitation & Cleanliness' then 1 end) as sanitation_complaints,
        
        -- Specific holiday-related complaints
        count(case when upper(complaint_type) like '%NOISE%' then 1 end) as all_noise_complaints,
        count(case when upper(complaint_type) like '%ILLEGAL FIREWORKS%' then 1 end) as fireworks_complaints,
        count(case when upper(complaint_type) like '%PARKING%' then 1 end) as parking_complaints,
        count(case when upper(complaint_type) like '%HEAT%' then 1 end) as heat_complaints,
        
        -- Response metrics
        avg(case when status = 'CLOSED' then response_time_hours end) as avg_response_time,
        round(count(case when response_sla_category = 'WITHIN_SLA' and status = 'CLOSED' then 1 end) * 100.0 / 
            nullif(count(case when status = 'CLOSED' then 1 end), 0), 2) as sla_compliance_rate
        
    from complaint_with_holidays
    where days_from_holiday is not null
    group by 1, 2, 3
),

-- Non-holiday baseline
baseline_metrics as (
    select
        year,
        month,
        avg(daily_count) as baseline_daily_avg,
        avg(noise_count) as baseline_noise_avg,
        avg(response_time) as baseline_response_avg
    from (
        select
            year,
            month,
            created_date_key,
            count(*) as daily_count,
            count(case when complaint_category = 'Noise' then 1 end) as noise_count,
            avg(case when status = 'CLOSED' then response_time_hours end) as response_time
        from complaint_with_holidays
        where days_from_holiday is null  -- Non-holiday periods
        group by 1, 2, 3
    ) daily
    group by 1, 2
),

-- Holiday-specific summaries
holiday_summary as (
    select
        nearest_holiday,
        holiday_category,
        
        -- Overall impact
        sum(complaint_count) as total_complaints_7day_window,
        avg(complaint_count) as avg_daily_complaints,
        max(complaint_count) as peak_day_complaints,
        min(complaint_count) as min_day_complaints,
        
        -- Pattern metrics
        sum(case when days_from_holiday < 0 then complaint_count end) as pre_holiday_complaints,
        sum(case when days_from_holiday = 0 then complaint_count end) as holiday_day_complaints,
        sum(case when days_from_holiday > 0 then complaint_count end) as post_holiday_complaints,
        
        -- Top complaint patterns
        sum(noise_complaints) as total_noise_complaints,
        sum(fireworks_complaints) as total_fireworks_complaints,
        sum(parking_complaints) as total_parking_complaints,
        
        -- Performance impact
        avg(avg_response_time) as avg_response_time_window,
        avg(sla_compliance_rate) as avg_sla_compliance
        
    from daily_holiday_pattern
    group by 1, 2
),

final as (
    select
        dhp.nearest_holiday,
        dhp.holiday_category,
        dhp.days_from_holiday,
        
        -- Volume metrics
        dhp.complaint_count,
        dhp.avg_holiday_complaints,
        round((dhp.complaint_count - dhp.avg_holiday_complaints) * 100.0 / 
            nullif(dhp.avg_holiday_complaints, 0), 2) as deviation_from_holiday_avg,
        
        -- Holiday impact indicators
        case
            when dhp.days_from_holiday = -1 and dhp.complaint_count > dhp.avg_holiday_complaints * 1.1 then 'Pre-Holiday Surge'
            when dhp.days_from_holiday = 0 and dhp.complaint_count < dhp.avg_holiday_complaints * 0.7 then 'Holiday Lull'
            when dhp.days_from_holiday = 1 and dhp.complaint_count > dhp.avg_holiday_complaints * 1.2 then 'Post-Holiday Peak'
            else 'Normal Pattern'
        end as holiday_pattern,
        
        -- Category distribution
        dhp.noise_complaints,
        round(dhp.noise_complaints * 100.0 / nullif(dhp.complaint_count, 0), 2) as noise_percentage,
        dhp.transportation_complaints,
        round(dhp.transportation_complaints * 100.0 / nullif(dhp.complaint_count, 0), 2) as transportation_percentage,
        dhp.housing_complaints,
        dhp.sanitation_complaints,
        
        -- Holiday-specific complaints
        dhp.fireworks_complaints,
        dhp.parking_complaints,
        dhp.heat_complaints,
        
        -- Response metrics
        dhp.avg_response_time,
        dhp.sla_compliance_rate,
        
        -- Holiday characteristics
        case
            when dhp.nearest_holiday = 'Independence Day' then true
            when dhp.nearest_holiday = 'New Year\'s Day' then true
            else false
        end as is_fireworks_holiday,
        
        case
            when dhp.nearest_holiday in ('Christmas Day', 'Thanksgiving') then true
            else false
        end as is_family_holiday,
        
        case
            when dhp.nearest_holiday in ('Memorial Day', 'Labor Day', 'Independence Day') then true
            else false
        end as is_summer_holiday,
        
        -- Metadata
        current_timestamp() as created_at
        
    from daily_holiday_pattern dhp
)

select * from final
order by nearest_holiday, days_from_holiday