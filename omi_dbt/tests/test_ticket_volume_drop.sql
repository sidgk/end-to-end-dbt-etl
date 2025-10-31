-- tests/test_ticket_volume_drop.sql
-- Test: Today's ticket volume should not drop >50% below the 7-day average

WITH ticket_counts AS (
    SELECT
        DATE(ticket_created_at) AS ticket_date,
        COUNT(*) AS total_tickets
    FROM {{ ref('dim_tickets') }}
    WHERE ticket_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
    GROUP BY 1
),

summary AS (
    SELECT
        MAX(CASE WHEN ticket_date = CURRENT_DATE() THEN total_tickets END) AS today_count,
        AVG(CASE WHEN ticket_date < CURRENT_DATE() THEN total_tickets END) AS avg_last_7_days
    FROM ticket_counts
)

SELECT *
FROM summary
WHERE today_count < (avg_last_7_days * 0.5)
