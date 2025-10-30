-- models/staging/stg_bookings.sql
WITH source AS(
SELECT
    bookingid AS booking_id,
    totalPrice AS booking_price,
    userSelectedCurrency AS booking_currency,
    status AS booking_status,
    partnerIdOffer AS partner_id_offer,
    createdAt AS created_at,
    updatedAt AS updated_at,
    raw_meta,
    -- get the latest records only
    ROW_NUMBER() OVER (PARTITION BY bookingid ORDER BY createdat desc) AS rn
FROM {{ source('omio', 'stage_bookings') }}
)
SELECT 
    *
FROM
    source
WHERE 
    rn = 1
