-- model raw_passangers.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='view',
    tags = ['stage'],
    enabled=true
) }}
WITH source AS(
SELECT
    passengerid AS passenger_id,
    bookingid AS booking_id,
    --  first_name, last_name, age are new fields added by Siddu
    INITCAP(firstName) AS first_name,
    INITCAP(lastName) AS last_name,
    age,
    type AS passenger_type,
    {{ var('dwh_loaddatetime') }} AS dwh_loaddatetime,
    ROW_NUMBER() OVER (PARTITION BY passengerid,bookingid ORDER BY passengerid) AS rn
FROM {{ source('omio', 'stage_passengers') }}
WHERE
    LOWER(passengerid) NOT LIKE 'pax%'
)
SELECT
    passenger_id,
    booking_id,
    first_name,
    last_name,
    passenger_type,
    age,
    dwh_loaddatetime
FROM 
    source
WHERE 
    rn = 1
