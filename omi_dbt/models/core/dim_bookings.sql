-- model dim_bookings.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='incremental',
    unique_key='booking_id',
    on_schema_change='sync_all_columns'
) }}

with source as (

    select 
        booking_id,
        booking_created_at,
        booking_updated_at,
        partner_id_offer,
        booking_price,
        booking_currency,
        booking_status,
        raw_meta,
        {{ var('dwh_loaddatetime') }} AS dwh_loaddatetime
    from {{ ref('raw_bookings') }}


    {% if is_incremental() %}
      -- load only records that are new or updated since last load
      where cast(created_at as timestamp) > (
        select coalesce(max(booking_created_at), timestamp('1900-01-01')) from {{ this }}
      )
      or cast(updated_at as timestamp) > (
        select coalesce(max(booking_updated_at), timestamp('1900-01-01')) from {{ this }}
      )
    {% endif %}
)

select * from source
