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
      -- only load new or updated records
      where created_at > (select max(created_at) from {{ this }})
    {% endif %}
)

select * from source
