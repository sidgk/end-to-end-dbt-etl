-- model raw_segments.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='view',
    tags = ['stage'],
    enabled=true
) }}
WITH source AS(
SELECT
    segmentid AS segment_id,
    bookingid AS booking_id,
    carriername AS carrier_name,
    departuredatetime AS departure_date_time,
    arrivaldatetime AS arrival_date_time,
    travelmode AS travel_mode,
    origin,
    destination,
    {{ var('dwh_loaddatetime') }} AS dwh_loaddatetime,
    ROW_NUMBER() OVER (PARTITION BY segmentid,bookingid ORDER BY segmentid) AS rn
FROM {{ source('omio', 'stage_segments') }}
WHERE 
    UPPER(segmentid) NOT LIKE 'SEG%'
)
SELECT
    segment_id,
    booking_id,
    carrier_name,
    departure_date_time,
    arrival_date_time,
    travel_mode,
    origin,
    destination,
    dwh_loaddatetime
FROM 
    source 
WHERE 
    rn = 1
