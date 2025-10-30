-- models/staging/raw_tickets.sql

{{ config(
    materialized='view'
) }}

SELECT
    ticketid,
    bookingid,
    bookingPrice,
    bookingCurrency,
    vendorCode,
    issuedAt,
    fareClass
FROM {{ source('omio', 'tickets') }}
WHERE DATE(issuedAt) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
