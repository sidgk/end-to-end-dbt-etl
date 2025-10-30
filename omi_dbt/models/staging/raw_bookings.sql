-- models/staging/stg_bookings.sql
SELECT
    bookingid AS booking_id,
    createdAt AS created_at,
    updatedAt AS updated_at,
    userSelectedCurrency AS booking_currency,
    partnerIdOffer AS partner_id_offer,
    totalPrice AS booking_price,
    raw_meta
FROM {{ source('omio', 'bookings') }}
