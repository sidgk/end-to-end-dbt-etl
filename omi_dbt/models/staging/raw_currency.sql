-- model raw_currency.sql
-- Author: Siddu Kattimani
{{ config(
    materialized='view',
    tags = ['stage'],
    enabled=true
) }}

SELECT
    date,
    currency_code,
    rate_to_eur
FROM {{ source('omio', 'stage_currency') }}
