-- model dim_segments.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='incremental',
    unique_key='segment_id',
    on_schema_change='sync_all_columns',
    tags=['dim', 'core']
) }}

WITH source AS (

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
    FROM {{ ref('raw_segments') }}

    {% if is_incremental() %}
        -- Only load rows newer than the latest already in the table
        where dwh_loaddatetime > (select max(dwh_loaddatetime) from {{ this }})
    {% endif %}

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
