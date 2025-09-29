-- Date Dimension Table
{{ 
  config(
    materialized='table',
    tags=['dimension', 'static']
  ) 
}}

with date_spine as (
    -- Generate all dates from 2010 to 2030
    select 
        explode(sequence(
            to_date('2010-01-01'), 
            to_date('2030-12-31'), 
            interval 1 day
        )) as date_actual
),

date_attributes as (
    select
        date_actual,
        
        -- Date key (YYYYMMDD format)
        cast(date_format(date_actual, 'yyyyMMdd') as int) as date_key,
        
        -- Year attributes
        year(date_actual) as year,
        quarter(date_actual) as quarter,
        
        -- Month attributes
        month(date_actual) as month,
        date_format(date_actual, 'MMMM') as month_name,
        date_format(date_actual, 'MMM') as month_short,
        
        -- Week attributes
        weekofyear(date_actual) as week_of_year,
        
        -- Day attributes
        day(date_actual) as day_of_month,
        dayofweek(date_actual) as day_of_week_num,
        date_format(date_actual, 'EEEE') as day_name,
        date_format(date_actual, 'EEE') as day_short,
        dayofyear(date_actual) as day_of_year,
        
        -- Weekend flag
        case 
            when dayofweek(date_actual) in (1, 7) then true 
            else false 
        end as is_weekend,
        
        -- NYC Fiscal Year (starts July 1)
        case
            when month(date_actual) >= 7 then year(date_actual) + 1
            else year(date_actual)
        end as fiscal_year,
        
        -- Fiscal Quarter
        case
            when month(date_actual) in (7, 8, 9) then 1
            when month(date_actual) in (10, 11, 12) then 2
            when month(date_actual) in (1, 2, 3) then 3
            when month(date_actual) in (4, 5, 6) then 4
        end as fiscal_quarter
        
    from date_spine
),

final as (
    select 
        date_key,
        date_actual,
        
        -- Standard calendar attributes
        year,
        quarter,
        month,
        month_name,
        month_short,
        week_of_year,
        day_of_month,
        day_of_week_num,
        day_name,
        day_short,
        day_of_year,
        
        -- Useful flags
        is_weekend,
        not is_weekend as is_weekday,
        
        -- NYC Fiscal calendar
        fiscal_year,
        fiscal_quarter,
        concat('FY', fiscal_year) as fiscal_year_name,
        concat('FY', fiscal_year, '-Q', fiscal_quarter) as fiscal_period,
        
        -- NYC Holidays (major ones)
        case
            when (month = 1 and day_of_month = 1) then 'New Year\'s Day'
            when (month = 1 and day_of_week_num = 2 and day_of_month between 15 and 21) then 'Martin Luther King Jr. Day'
            when (month = 2 and day_of_week_num = 2 and day_of_month between 15 and 21) then 'Presidents\' Day'
            when (month = 5 and day_of_week_num = 2 and day_of_month between 25 and 31) then 'Memorial Day'
            when (month = 6 and day_of_month = 19) then 'Juneteenth'
            when (month = 7 and day_of_month = 4) then 'Independence Day'
            when (month = 9 and day_of_week_num = 2 and day_of_month between 1 and 7) then 'Labor Day'
            when (month = 10 and day_of_week_num = 2 and day_of_month between 8 and 14) then 'Columbus Day'
            when (month = 11 and day_of_month = 11) then 'Veterans Day'
            when (month = 11 and day_of_week_num = 5 and day_of_month between 22 and 28) then 'Thanksgiving'
            when (month = 12 and day_of_month = 25) then 'Christmas Day'
            else null
        end as holiday_name,
        
        case 
            when holiday_name is not null then true 
            else false 
        end as is_holiday,
        
        -- Business day flag
        case 
            when is_weekend then false
            when is_holiday then false
            else true
        end as is_business_day,
        
        -- Relative date indicators
        case
            when date_actual = current_date() then 'Today'
            when date_actual = current_date() - 1 then 'Yesterday'
            when date_actual = current_date() + 1 then 'Tomorrow'
            when date_actual between current_date() - 7 and current_date() - 1 then 'Last 7 Days'
            when date_actual between current_date() - 30 and current_date() - 1 then 'Last 30 Days'
            else 'Other'
        end as relative_date,
        
        -- Metadata
        current_timestamp() as created_at
        
    from date_attributes
)

select * from final
order by date_key