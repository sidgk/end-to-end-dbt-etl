-- model: bridge_ticket_passenger.sql
-- Author: Siddu Kattimani

{{ config(
    materialized='table',
    tags=['bridge']
) }}

WITH ticket AS (
    SELECT ticket_id, booking_id FROM {{ ref('dim_tickets') }}
),
passenger AS (
    SELECT passenger_id, booking_id FROM {{ ref('dim_passengers') }}
)

SELECT 
    t.ticket_id,
    p.passenger_id
FROM ticket t
JOIN passenger p
  ON t.booking_id = p.booking_id
