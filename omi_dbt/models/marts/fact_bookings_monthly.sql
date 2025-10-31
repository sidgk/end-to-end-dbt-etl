-- models/marts/fact_bookings_monthly.sql
-- Author: Siddu Kattimani
-- Purpose: Monthly summary fact table aggregating booking, passenger, and ticket activity

{{ config(
    materialized='table',
    schema='marts',
    tags=['mart']
) }}

with base as (
    select
        dd.date_day as date,
        count(distinct fb.booking_id) as total_bookings,
        count(distinct t.ticket_id) as total_tickets_sold,
        count(distinct p.passenger_id) as total_passengers,
        sum(fb.booking_price) as total_booking_value,
        avg(fb.booking_price) as avg_booking_value
    from {{ ref('dim_date') }} dd
    left join {{ ref('fact_booking') }} fb
        on date(fb.booking_created_at) = dd.date_day
    left join {{ ref('dim_tickets') }} t
        on fb.booking_id = t.booking_id
    left join {{ ref('bridge_ticket_passenger') }} btp
        on t.ticket_id = btp.ticket_id
    left join {{ ref('dim_passengers') }} p
        on btp.passenger_id = p.passenger_id
    where 
    -- hardcoaded it just as an example. 
       dd.date_day > '2025-09-01'
    group by dd.date_day
)

select
    date,
    sum(total_bookings) as total_bookings,
    sum(total_tickets_sold) as total_tickets_sold,
    sum(total_passengers) as total_passengers,
    sum(total_booking_value) as total_booking_value,
    avg(avg_booking_value) as avg_booking_value
from base
group by 1
