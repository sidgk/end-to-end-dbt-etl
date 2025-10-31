-- model raw_bookings.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='view',
    tags = ['stage'],
    enabled=true
) }}
WITH source AS(
SELECT
    bookingid AS booking_id,
    createdAt AS booking_created_at,
    updatedAt AS booking_updated_at,
    userSelectedCurrency AS booking_currency,
    totalPrice AS booking_price,
    {{ convert_to_eur('userSelectedCurrency', 'totalPrice', 'createdAt') }} as booking_price_eur,
    partnerIdOffer AS partner_id_offer,
    --  status and raw_meta are new fields adeed by Siddu
    status AS booking_status,
    raw_meta,
    {{ var('dwh_loaddatetime') }} AS dwh_loaddatetime,
    -- get the latest records only
    ROW_NUMBER() OVER (PARTITION BY bookingid ORDER BY createdat desc) AS rn
FROM {{ source('omio', 'stage_bookings') }}
)
SELECT 
    booking_id,
    booking_price,
    booking_currency,
    partner_id_offer,
    booking_created_at,
    booking_updated_at,
    booking_status,
    raw_meta,
    dwh_loaddatetime
FROM
    source as bookings
WHERE 
    rn = 1
