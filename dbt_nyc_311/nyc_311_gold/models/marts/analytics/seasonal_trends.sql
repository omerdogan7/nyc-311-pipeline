-- Seasonal Trends Analysis on NYC 311 Complaints
{{ 
  config(
    materialized='table',
    tags=['analytics', 'seasonal', 'trends']
  ) 
}}

with seasonal_classification as (
    select
        d.date_key,
        d.date_actual,
        d.year,
        d.month,
        d.month_name,
        d.day_of_year,
        d.week_of_year,
        
        -- Season classification
        case
            when d.month in (12, 1, 2) then 'Winter'
            when d.month in (3, 4, 5) then 'Spring'  
            when d.month in (6, 7, 8) then 'Summer'
            when d.month in (9, 10, 11) then 'Fall'
        end as season,
        
        -- More granular seasonal periods
        case
            when d.month = 12 or d.month <= 2 then 'Deep Winter'
            when d.month = 3 then 'Early Spring'
            when d.month in (4, 5) then 'Late Spring'
            when d.month in (6, 7) then 'Early Summer'
            when d.month = 8 then 'Late Summer'
            when d.month = 9 then 'Early Fall'
            when d.month in (10, 11) then 'Late Fall'
        end as seasonal_period,
        
        -- NYC-specific seasonal characteristics
        case
            when d.month in (12, 1, 2) then 'Heating Season'
            when d.month in (6, 7, 8) then 'Outdoor Activity Season'
            when d.month in (4, 5, 9, 10) then 'Mild Weather Season'
            when d.month in (3, 11) then 'Transition Season'
        end as nyc_seasonal_context,
        
        -- Holiday-heavy periods
        case
            when d.month = 12 then 'Holiday Season'
            when d.month in (6, 7, 8) then 'Summer Break'
            when d.month = 9 then 'Back to School'
            else 'Regular Period'
        end as activity_period
        
    from {{ ref('dim_date') }} d
    where d.year between 2020 and 2023
),

complaint_seasonal_data as (
    select
        f.*,
        sc.season,
        sc.seasonal_period,
        sc.nyc_seasonal_context,
        sc.activity_period,
        sc.month,
        sc.month_name,
        sc.year,
        a.agency_name,
        a.agency_category,
        l.borough,
        l.borough_category,
        ct.complaint_category,
        ct.complaint_type
    from {{ ref('fct_complaints') }} f
    inner join seasonal_classification sc on f.created_date_key = sc.date_key
    inner join {{ ref('dim_agency') }} a on f.agency_key = a.agency_key
    inner join {{ ref('dim_location') }} l on f.location_key = l.location_key
    inner join {{ ref('dim_complaint_type') }} ct on f.complaint_type_key = ct.complaint_type_key
),

-- Monthly seasonal patterns
monthly_patterns as (
    select
        month,
        month_name,
        season,
        nyc_seasonal_context,
        
        -- Volume metrics
        count(*) as total_complaints,
        avg(count(*)) over () as annual_avg_complaints,
        
        -- Complaint category distribution
        count(case when complaint_category = 'Noise' then 1 end) as noise_complaints,
        count(case when complaint_category = 'Building & Housing' then 1 end) as housing_complaints,
        count(case when complaint_category = 'Parks & Environment' then 1 end) as parks_complaints,
        count(case when complaint_category = 'Street & Infrastructure' then 1 end) as street_complaints,
        count(case when complaint_category = 'Transportation' then 1 end) as transportation_complaints,
        count(case when complaint_category = 'Sanitation & Cleanliness' then 1 end) as sanitation_complaints,
        
        -- Weather-related complaint proxies
        count(case when upper(complaint_type) like '%HEAT%' or upper(complaint_type) like '%HOT WATER%' then 1 end) as heating_complaints,
        count(case when upper(complaint_type) like '%SNOW%' or upper(complaint_type) like '%ICE%' then 1 end) as winter_weather_complaints,
        count(case when upper(complaint_type) like '%AIR%' or upper(complaint_type) like '%COOLING%' then 1 end) as cooling_complaints,
        count(case when upper(complaint_type) like '%TREE%' or upper(complaint_type) like '%BRANCH%' then 1 end) as tree_complaints,
        
        -- Performance metrics
        avg(case when status = 'CLOSED' then response_time_hours end) as avg_response_time,
        round(count(case when response_sla_category = 'WITHIN_SLA' and status = 'CLOSED' then 1 end) * 100.0 / 
            nullif(count(case when status = 'CLOSED' then 1 end), 0), 2) as sla_compliance_rate
            
    from complaint_seasonal_data
    group by 1, 2, 3, 4
),

-- Seasonal averages for comparison
seasonal_averages as (
    select
        season,
        avg(total_complaints) as season_avg_complaints,
        avg(noise_complaints) as season_avg_noise,
        avg(housing_complaints) as season_avg_housing,
        avg(heating_complaints) as season_avg_heating,
        avg(avg_response_time) as season_avg_response_time
    from monthly_patterns
    group by 1
),

-- Borough-level seasonal patterns
borough_seasonal as (
    select
        borough,
        season,
        count(*) as total_complaints,
        round(count(*) * 100.0 / sum(count(*)) over (partition by season), 2) as pct_of_season_total,
        
        -- Top complaint categories by borough and season
        count(case when complaint_category = 'Noise' then 1 end) as noise_count,
        count(case when complaint_category = 'Building & Housing' then 1 end) as housing_count,
        round(count(case when complaint_category = 'Noise' then 1 end) * 100.0 / count(*), 2) as noise_pct,
        round(count(case when complaint_category = 'Building & Housing' then 1 end) * 100.0 / count(*), 2) as housing_pct
        
    from complaint_seasonal_data
    group by 1, 2
),

-- Year-over-year seasonal comparison
yoy_seasonal as (
    select
        year,
        season,
        count(*) as complaints,
        lag(count(*)) over (partition by season order by year) as prev_year_complaints,
        round((count(*) - lag(count(*)) over (partition by season order by year)) * 100.0 / 
            nullif(lag(count(*)) over (partition by season order by year), 0), 2) as yoy_growth_rate
    from complaint_seasonal_data
    group by 1, 2
),

final as (
    select
        mp.month,
        mp.month_name,
        mp.season,
        mp.nyc_seasonal_context,
        
        -- Volume analysis
        mp.total_complaints,
        mp.annual_avg_complaints,
        round((mp.total_complaints - mp.annual_avg_complaints) * 100.0 / mp.annual_avg_complaints, 2) as monthly_deviation_pct,
        
        sa.season_avg_complaints,
        round((mp.total_complaints - sa.season_avg_complaints) * 100.0 / sa.season_avg_complaints, 2) as seasonal_deviation_pct,
        
        -- Category breakdowns with seasonal context
        mp.noise_complaints,
        round(mp.noise_complaints * 100.0 / mp.total_complaints, 2) as noise_percentage,
        sa.season_avg_noise,
        
        mp.housing_complaints,  
        round(mp.housing_complaints * 100.0 / mp.total_complaints, 2) as housing_percentage,
        sa.season_avg_housing,
        
        mp.parks_complaints,
        mp.street_complaints,
        mp.transportation_complaints,
        mp.sanitation_complaints,
        
        -- Weather-related seasonal indicators
        mp.heating_complaints,
        round(mp.heating_complaints * 100.0 / mp.total_complaints, 2) as heating_complaint_pct,
        sa.season_avg_heating,
        
        mp.winter_weather_complaints,
        mp.cooling_complaints,
        mp.tree_complaints,
        
        -- Performance seasonality
        round(mp.avg_response_time, 2) as avg_response_time,
        round(sa.season_avg_response_time, 2) as season_avg_response_time,
        mp.sla_compliance_rate,
        
        -- Seasonal pattern indicators
        case
            when mp.month in (1, 2, 12) and mp.heating_complaints > mp.total_complaints * 0.15 then 'High Winter Demand'
            when mp.month in (6, 7, 8) and mp.noise_complaints > mp.total_complaints * 0.3 then 'Summer Noise Peak'
            when mp.month in (4, 5) and mp.tree_complaints > sa.season_avg_complaints * 0.05 then 'Spring Maintenance Season'
            when mp.month in (9, 10) and mp.street_complaints > sa.season_avg_complaints * 0.2 then 'Fall Infrastructure Focus'
            else 'Normal Seasonal Pattern'
        end as seasonal_pattern_indicator,
        
        -- Seasonality strength score (how much this month deviates from annual average)
        round(abs(mp.total_complaints - mp.annual_avg_complaints) * 100.0 / mp.annual_avg_complaints, 2) as seasonality_strength,
        
        -- Climate adaptation indicators
        case
            when mp.heating_complaints + mp.cooling_complaints > mp.total_complaints * 0.2 then 'Climate Sensitive'
            when mp.winter_weather_complaints + mp.tree_complaints > mp.total_complaints * 0.1 then 'Weather Sensitive'
            else 'Climate Stable'
        end as climate_sensitivity,
        
        -- Metadata
        current_timestamp() as created_at
        
    from monthly_patterns mp
    left join seasonal_averages sa on mp.season = sa.season
)

select * from final
order by month