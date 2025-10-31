-- tests/test_bookings_have_tickets.sql
-- Business logic test: every booking should have at least one ticket

SELECT
    b.booking_id
FROM {{ ref('dim_bookings') }} AS b
LEFT JOIN {{ ref('dim_tickets') }} AS t
    ON b.booking_id = t.booking_id
WHERE t.ticket_id IS NULL
