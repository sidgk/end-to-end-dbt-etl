-- model: fact_booking.sql
-- Author: Siddu Kattimani

{{ config(
    materialized='table',
    unique_key='booking_id',
    on_schema_change='sync_all_columns',
    tags=['fact','core']
) }}

-- Base sources
WITH bookings AS (
    SELECT 
        booking_id,
        booking_created_at,
        booking_price,
        dwh_loaddatetime
    FROM {{ ref('dim_bookings') }}
    WHERE 
        booking_status IS NOT NULL
        AND partner_id_offer IS NOT NULL
        AND booking_price IS NOT NULL
)

,tickets AS (
    SELECT 
        booking_id,
        COUNT(DISTINCT ticket_id) AS ticket_count
    FROM {{ ref('dim_tickets') }}
    GROUP BY booking_id
)

,segments AS (
    SELECT 
        booking_id,
        COUNT(DISTINCT segment_id) AS segment_count
    FROM {{ ref('dim_segments') }}
    GROUP BY booking_id
)

,passengers AS (
    SELECT 
        booking_id,
        COUNT(DISTINCT passenger_id) AS passenger_count
    FROM {{ ref('dim_passengers') }}
    GROUP BY booking_id
)

SELECT 
    b.booking_id,
    b.booking_created_at,
    b.booking_price,
    COALESCE(t.ticket_count, 0) AS ticket_count,
    COALESCE(s.segment_count, 0) AS segment_count_per_booking,
    COALESCE(p.passenger_count, 0) AS passenger_count,
    {{ var('dwh_loaddatetime') }} AS dwh_loaddatetime
FROM bookings b
LEFT JOIN tickets t USING (booking_id)
LEFT JOIN segments s USING (booking_id)
LEFT JOIN passengers p USING (booking_id)

-- {% if is_incremental() %}
-- WHERE b.dwh_loaddatetime > (SELECT MAX(dwh_loaddatetime) FROM {{ this }})
-- {% endif %}
