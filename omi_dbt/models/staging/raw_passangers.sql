-- models/staging/stg_passengers.sql
SELECT
    passengerid AS passenger_id,
    bookingid AS booking_id,
    INITCAP(firstName) AS first_name,
    INITCAP(lastName) AS last_name,
    type AS passenger_type,
    age
FROM {{ source('omio', 'passengers') }}
