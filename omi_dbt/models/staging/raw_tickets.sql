-- model raw_tickets.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='view',
    tags = ['stage'],
    enabled=true
) }}
WITH source AS(
SELECT
    ticketid AS ticket_id,
    bookingid AS booking_id,
    bookingPrice AS booking_price,
    bookingCurrency AS booking_currency,
    vendorCode AS vendor_code,
    -- issuedAt and fareClass are the columns added by siddu
    -- this helps in understanding how much time did the ventor took to issue the ticket i.e. difference between booing_created at and issuedat
    issuedAt AS issued_at,
    -- A person might change the class after the ticket has been booked and we want to keep track of those changes by taking the snapshots
    -- Ex: Economy, Standard, Business
    fareClass,
    {{ var('dwh_loaddatetime') }} AS dwh_loaddatetime,
    ROW_NUMBER() OVER (PARTITION BY ticketid,bookingid ORDER BY ticketid) AS rn
FROM {{ source('omio', 'stage_tickets') }}
WHERE
    LOWER(ticketid) NOT LIKE 'tck%'
)
SELECT
    ticket_id,
    booking_id,
    booking_price,
    booking_currency,
    vendor_code,
    issued_at,
    fareClass,
    dwh_loaddatetime
FROM 
    source 
WHERE 
    rn = 1