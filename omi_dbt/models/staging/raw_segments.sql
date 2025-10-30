-- models/staging/raw_segments.sql

{{ config(
    materialized='view'
) }}

SELECT
    segmentid AS segment_id,
    bookingid AS booking_id,
    carriername AS carrier_name,
    departuredatetime,
    arrivaldatetime,
    travelmode,
    origin,
    destination
FROM {{ source('omio', 'stage_segments') }}
