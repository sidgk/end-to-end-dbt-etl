-- model: dim_passengers.sql
-- Author: Siddu Kattimani

{{ config(
    materialized = 'incremental',
    unique_key = 'passenger_id',
    on_schema_change = 'sync_all_columns',
    tags = ['dim', 'core']
) }}

WITH source AS (

    SELECT
        passenger_id,
        booking_id,
        first_name,
        last_name,
        passenger_type,
        age,
        dwh_loaddatetime
    FROM {{ ref('raw_passangers') }}

    {% if is_incremental() %}
        where dwh_loaddatetime > (
            select coalesce(max(dwh_loaddatetime), '1900-01-01') from {{ this }}
        )
    {% endif %}

)

SELECT
    passenger_id,
    booking_id,
    first_name,
    last_name,
    passenger_type,
    age,
    dwh_loaddatetime
FROM source
