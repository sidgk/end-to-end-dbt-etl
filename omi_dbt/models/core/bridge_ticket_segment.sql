-- model: bridge_ticket_segment.sql
-- Author: Siddu Kattimani

{{ config(
    materialized='table',
    tags=['bridge']
) }}

WITH ticket AS (
    SELECT ticket_id, booking_id FROM {{ ref('dim_tickets') }}
),
segment AS (
    SELECT segment_id, booking_id FROM {{ ref('dim_segments') }}
)

SELECT 
    t.ticket_id,
    s.segment_id
FROM ticket t
JOIN segment s
  ON t.booking_id = s.booking_id
