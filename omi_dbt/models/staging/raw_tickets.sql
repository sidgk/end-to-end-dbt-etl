-- models/staging/raw_tickets.sql

{{ config(
    materialized='view'
) }}

SELECT
    ticketid AS ticket_id,
    bookingid AS booking_id,
    bookingPrice AS booking_price,
    bookingCurrency AS booking_currency,
    vendorCode AS vendor_code,
    issuedAt as issued_at,
    fareClass
FROM {{ source('omio', 'stage_tickets') }}
