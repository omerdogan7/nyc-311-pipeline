-- COVID-19 Impact Analysis on NYC 311 Complaints
{{ 
  config(
    materialized='table',
    tags=['analytics', 'covid']
  ) 
}}

with date_periods as (
    select
        date_key,
        date_actual,
        year,
        month,
        case 
            when date_actual between '2019-01-01' and '2020-03-15' then 'Pre-COVID'
            when date_actual between '2020-03-16' and '2021-06-30' then 'COVID Peak'
            when date_actual >= '2021-07-01' then 'Post-COVID'
            else 'Other'
        end as covid_period,
        case 
            when date_actual between '2020-03-16' and '2021-06-30' then true 
            else false 
        end as is_covid_period
    from {{ ref('dim_date') }}
    where year between 2019 and 2023
),

complaint_base as (
    select
        f.*,
        dp.covid_period,
        dp.is_covid_period,
        dp.year,
        dp.month,
        a.agency_name,
        a.agency_category,
        l.borough,
        ct.complaint_category,
        ct.complaint_type
    from {{ ref('fct_complaints') }} f
    inner join date_periods dp on f.created_date_key = dp.date_key
    inner join {{ ref('dim_agency') }} a on f.agency_key = a.agency_key
    inner join {{ ref('dim_location') }} l on f.location_key = l.location_key
    inner join {{ ref('dim_complaint_type') }} ct on f.complaint_type_key = ct.complaint_type_key
),

-- Monthly aggregations by COVID period
monthly_covid_impact as (
    select
        year,
        month,
        covid_period,
        
        -- Volume metrics
        count(*) as total_complaints,
        count(case when status = 'CLOSED' then 1 end) as closed_complaints,
        
        -- Performance metrics
        avg(case when status = 'CLOSED' then response_time_hours end) as avg_response_time,
        round(count(case when response_sla_category = 'WITHIN_SLA' and status = 'CLOSED' then 1 end) * 100.0 / 
            nullif(count(case when status = 'CLOSED' then 1 end), 0), 2) as sla_compliance_rate,
            
        -- Category breakdowns
        count(case when complaint_category = 'Noise' then 1 end) as noise_complaints,
        count(case when complaint_category = 'Transportation' then 1 end) as transportation_complaints,
        count(case when complaint_category = 'Building & Housing' then 1 end) as housing_complaints,
        count(case when complaint_category = 'Sanitation & Cleanliness' then 1 end) as sanitation_complaints,
        count(case when complaint_category = 'Quality of Life' then 1 end) as quality_of_life_complaints,
        
        -- Specific COVID-related patterns
        count(case when upper(complaint_type) like '%COVID%' then 1 end) as covid_specific_complaints,
        count(case when upper(complaint_type) like '%NOISE - RESIDENTIAL%' then 1 end) as residential_noise_complaints,
        count(case when upper(complaint_type) like '%OUTDOOR DINING%' then 1 end) as outdoor_dining_complaints
        
    from complaint_base
    group by 1, 2, 3
),

-- Pre-COVID baseline calculation (2019 average)
pre_covid_baseline as (
    select
        month,
        avg(total_complaints) as baseline_monthly_complaints,
        avg(noise_complaints) as baseline_noise_complaints,
        avg(transportation_complaints) as baseline_transportation_complaints,
        avg(housing_complaints) as baseline_housing_complaints,
        avg(avg_response_time) as baseline_response_time
    from monthly_covid_impact
    where covid_period = 'Pre-COVID' and year = 2019
    group by 1
),

-- Borough-level COVID impact
borough_covid_impact as (
    select
        borough,
        covid_period,
        count(*) as complaints_count,
        round(avg(case when status = 'CLOSED' then response_time_hours end), 2) as avg_response_time,
        count(case when complaint_category = 'Noise' then 1 end) as noise_count,
        round(count(case when complaint_category = 'Noise' then 1 end) * 100.0 / count(*), 2) as noise_percentage
    from complaint_base
    group by 1, 2
),

final as (
    select
        mci.year,
        mci.month,
        mci.covid_period,
        
        -- Volume metrics
        mci.total_complaints,
        pcb.baseline_monthly_complaints,
        round((mci.total_complaints - pcb.baseline_monthly_complaints) * 100.0 / 
            nullif(pcb.baseline_monthly_complaints, 0), 2) as volume_change_pct,
        
        -- Performance impact
        mci.avg_response_time,
        pcb.baseline_response_time,
        round((mci.avg_response_time - pcb.baseline_response_time) * 100.0 / 
            nullif(pcb.baseline_response_time, 0), 2) as response_time_change_pct,
        
        mci.sla_compliance_rate,
        
        -- Category shifts
        mci.noise_complaints,
        pcb.baseline_noise_complaints,
        round((mci.noise_complaints - pcb.baseline_noise_complaints) * 100.0 / 
            nullif(pcb.baseline_noise_complaints, 0), 2) as noise_change_pct,
            
        mci.transportation_complaints,
        pcb.baseline_transportation_complaints,
        round((mci.transportation_complaints - pcb.baseline_transportation_complaints) * 100.0 / 
            nullif(pcb.baseline_transportation_complaints, 0), 2) as transportation_change_pct,
            
        mci.housing_complaints,
        mci.sanitation_complaints,
        mci.quality_of_life_complaints,
        
        -- COVID-specific metrics
        mci.covid_specific_complaints,
        mci.residential_noise_complaints,
        mci.outdoor_dining_complaints,
        
        -- Pattern indicators
        case 
            when mci.noise_complaints > pcb.baseline_noise_complaints * 1.2 then 'High Noise Period'
            when mci.transportation_complaints < pcb.baseline_transportation_complaints * 0.8 then 'Low Transit Period'
            when mci.total_complaints < pcb.baseline_monthly_complaints * 0.9 then 'Low Activity Period'
            else 'Normal Pattern'
        end as covid_pattern_indicator,
        
        -- WFH impact score (proxy: residential noise vs transportation ratio)
        round(mci.noise_complaints * 1.0 / nullif(mci.transportation_complaints, 1), 2) as wfh_impact_score,
        
        -- Metadata
        current_timestamp() as created_at
        
    from monthly_covid_impact mci
    left join pre_covid_baseline pcb on mci.month = pcb.month
)

select * from final
order by year, month