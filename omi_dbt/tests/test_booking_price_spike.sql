-- tests/test_booking_price_spike.sql
-- Author: Siddu Kattimani
-- Purpose: Detect unusual spikes in average booking price

with historical as (
    select 
        avg(booking_price) as avg_booking_price_7d
    from {{ ref('fact_booking') }}
    where date(booking_created_at) between current_date - interval '7 day' and current_date - interval '1 day'
),
today as (
    select 
        avg(booking_price) as avg_booking_price_today
    from {{ ref('fact_booking') }}
    where date(booking_created_at) = current_date
)
select
    case 
        when today.avg_booking_price_today > (historical.avg_booking_price_7d * 2.5)
        then 1
        else 0
    end as anomaly_flag
from today, historical
where today.avg_booking_price_today is not null
  and historical.avg_booking_price_7d is not null
