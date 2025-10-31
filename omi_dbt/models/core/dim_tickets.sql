-- model: dim_tickets.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='sync_all_columns',
    tags=['dim', 'core']
) }}

with source as (

    select
        ticket_id,
        booking_id,
        booking_price,
        booking_currency,
        vendor_code,
        issued_at,
        fareClass as fare_class,
        dwh_loaddatetime
    from {{ ref('raw_tickets') }}

    {% if is_incremental() %}
        where dwh_loaddatetime > (
            select coalesce(max(dwh_loaddatetime), '1900-01-01') from {{ this }}
        )
    {% endif %}
)

select
    ticket_id,
    booking_id,
    booking_price,
    booking_currency,
    vendor_code,
    issued_at,
    fare_class,
    dwh_loaddatetime
from source