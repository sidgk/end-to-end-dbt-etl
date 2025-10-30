-- models/staging/stg_bookings.sql
SELECT
    bookingid,
    createdAt,
    updatedAt,
    userSelectedCurrency,
    partnerIdOffer,
    totalPrice,
    raw_meta
FROM {{ source('omio', 'bookings') }}
