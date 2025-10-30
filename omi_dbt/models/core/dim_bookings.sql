{{ config(
    materialized='incremental',
    unique_key='booking_id',
    on_schema_change='sync_all_columns'
) }}

with source as (

    select 
        booking_id,
        created_at,
        updated_at,
        partner_id_offer,
        booking_price,
        booking_currency,
        current_timestamp() as dwh_load_time
    from {{ ref('raw_bookings') }}


    {% if is_incremental() %}
      -- load only records that are new or updated since last load
      where cast(created_at as timestamp) > (
        select coalesce(max(created_at), timestamp('1900-01-01')) from {{ this }}
      )
      or cast(updated_at as timestamp) > (
        select coalesce(max(updated_at), timestamp('1900-01-01')) from {{ this }}
      )
    {% endif %}
)

select * from source
