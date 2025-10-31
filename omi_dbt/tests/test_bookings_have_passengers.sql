-- tests/test_bookings_have_passengers.sql
-- Business logic test: every booking should have at least one passenger

SELECT
    b.booking_id
FROM {{ ref('dim_bookings') }} AS b
LEFT JOIN {{ ref('bridge_ticket_passenger') }} AS bp
    ON b.booking_id = bp.ticket_id  -- assuming ticket_id links to booking_id via ticket
LEFT JOIN {{ ref('dim_tickets') }} AS t
    ON bp.ticket_id = t.ticket_id
WHERE bp.passenger_id IS NULL
