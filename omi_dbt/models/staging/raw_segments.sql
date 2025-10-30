-- models/staging/raw_segments.sql

{{ config(
    materialized='view'
) }}

SELECT
    segmentid,
    bookingid,
    carriername,
    departuredatetime,
    arrivaldatetime,
    travelmode,
    origin,
    destination
FROM {{ source('omio', 'segments') }}
WHERE DATE(departuredatetime) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
