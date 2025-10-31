-- Date Dimension Table (Improved)
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
        
        -- Date key (YYYYMMDD format - INTEGER for fast joins)
        cast(date_format(date_actual, 'yyyyMMdd') as int) as date_key,
        
        -- Year attributes
        year(date_actual) as year,
        quarter(date_actual) as quarter,
        concat('Q', quarter(date_actual), '-', year(date_actual)) as quarter_name,
        
        -- Month attributes
        month(date_actual) as month,
        date_format(date_actual, 'MMMM') as month_name,
        date_format(date_actual, 'MMM') as month_short,
        concat(year(date_actual), '-', lpad(month(date_actual), 2, '0')) as year_month,
        
        -- Week attributes
        weekofyear(date_actual) as week_of_year,
        
        -- Day attributes
        day(date_actual) as day_of_month,
        dayofweek(date_actual) as day_of_week_num, -- 1=Sunday, 7=Saturday
        date_format(date_actual, 'EEEE') as day_name,
        date_format(date_actual, 'EEE') as day_short,
        dayofyear(date_actual) as day_of_year,
        
        -- Weekend flag (1=Sunday, 7=Saturday in Spark)
        case 
            when dayofweek(date_actual) in (1, 7) then true 
            else false 
        end as is_weekend,
        
        -- NYC Fiscal Year (starts July 1)
        case
            when month(date_actual) >= 7 then year(date_actual) + 1
            else year(date_actual)
        end as fiscal_year,
        
        -- Fiscal Quarter (Q1: Jul-Sep, Q2: Oct-Dec, Q3: Jan-Mar, Q4: Apr-Jun)
        case
            when month(date_actual) in (7, 8, 9) then 1
            when month(date_actual) in (10, 11, 12) then 2
            when month(date_actual) in (1, 2, 3) then 3
            when month(date_actual) in (4, 5, 6) then 4
        end as fiscal_quarter,
        
        -- NYC Holidays (FIXED: No self-reference)
        case
            when (month(date_actual) = 1 and day(date_actual) = 1) then 'New Year\'s Day'
            when (month(date_actual) = 1 and dayofweek(date_actual) = 2 and day(date_actual) between 15 and 21) then 'Martin Luther King Jr. Day'
            when (month(date_actual) = 2 and dayofweek(date_actual) = 2 and day(date_actual) between 15 and 21) then 'Presidents\' Day'
            when (month(date_actual) = 5 and dayofweek(date_actual) = 2 and day(date_actual) between 25 and 31) then 'Memorial Day'
            when (month(date_actual) = 6 and day(date_actual) = 19) then 'Juneteenth'
            when (month(date_actual) = 7 and day(date_actual) = 4) then 'Independence Day'
            when (month(date_actual) = 9 and dayofweek(date_actual) = 2 and day(date_actual) between 1 and 7) then 'Labor Day'
            when (month(date_actual) = 10 and dayofweek(date_actual) = 2 and day(date_actual) between 8 and 14) then 'Columbus Day'
            when (month(date_actual) = 11 and day(date_actual) = 11) then 'Veterans Day'
            when (month(date_actual) = 11 and dayofweek(date_actual) = 5 and day(date_actual) between 22 and 28) then 'Thanksgiving'
            when (month(date_actual) = 12 and day(date_actual) = 25) then 'Christmas Day'
            else null
        end as holiday_name
        
    from date_spine
),

final as (
    select 
        -- Primary Key
        date_key,
        date_actual,
        
        -- Standard calendar attributes
        year,
        quarter,
        quarter_name,
        month,
        month_name,
        month_short,
        year_month,
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
        
        -- Holiday information (NOW SAFE TO REFERENCE)
        holiday_name,
        case when holiday_name is not null then true else false end as is_holiday,
        
        -- Business day flag (excludes weekends AND holidays)
        case 
            when is_weekend then false
            when holiday_name is not null then false
            else true
        end as is_business_day,
        
        -- Relative date indicators (optimized)
        case
            when date_actual = current_date() then 'Today'
            when date_actual = date_add(current_date(), -1) then 'Yesterday'
            when date_actual = date_add(current_date(), 1) then 'Tomorrow'
            when date_actual >= date_add(current_date(), -7) and date_actual < current_date() then 'Last 7 Days'
            when date_actual >= date_add(current_date(), -30) and date_actual < current_date() then 'Last 30 Days'
            when date_actual >= date_add(current_date(), -90) and date_actual < current_date() then 'Last 90 Days'
            when date_actual < current_date() then 'Past'
            when date_actual > current_date() then 'Future'
            else 'Other'
        end as relative_date,
        
        -- Season (meteorological)
        case
            when month in (12, 1, 2) then 'Winter'
            when month in (3, 4, 5) then 'Spring'
            when month in (6, 7, 8) then 'Summer'
            when month in (9, 10, 11) then 'Fall'
        end as season,
        
        -- Metadata
        current_timestamp() as created_at
        
    from date_attributes
)

select * from final
order by date_key

-- Expected: ~7,670 rows (2010-2030 = 21 years × 365.25 days)