{% macro convert_to_eur(currency_code, amount, booking_date) %}
(
    case
        when {{ currency_code }} = 'EUR' then {{ amount }}
        else (
            {{ amount }} *
            coalesce((
                select rate_to_eur
                from {{ ref('currency_snapshot') }}
                where currency_code = {{ currency_code }}
                  and date <= cast({{ booking_date }} as date)
                order by date desc
                limit 1
            ), 1)
        )
    end
)
{% endmacro %}
