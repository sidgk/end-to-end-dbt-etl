-- models/core/dim_date.sql
-- Author: Siddu Kattimani
-- Purpose: Date dimension table for consistent time-based analysis.

{{ config(
    materialized='table',
    tags=['core']
) }}

with calendar as (
    -- Generate a list of dates from 2020-01-01 to 2028-12-31
    select
        date_add('2020-01-01', interval n day) as date_day
    from unnest(generate_array(0, date_diff('2028-12-31', '2020-01-01', day))) as n
),

date_attributes as (
    select
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month_number,
        format_date('%B', date_day) as month_name,
        extract(day from date_day) as day_of_month,
        extract(quarter from date_day) as quarter,
        extract(week from date_day) as week_of_year,
        extract(dayofweek from date_day) as day_of_week_number, -- 1=Sunday ... 7=Saturday
        format_date('%A', date_day) as day_of_week_name,
        format_date('%Y-%m', date_day) as year_month,
        format_date('%Y-%m-%d', date_day) as full_date_string
    from calendar
)

select * from date_attributes
order by date_day
