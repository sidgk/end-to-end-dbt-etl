{% snapshot booking_status_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='booking_id',
    strategy='timestamp',
    updated_at='booking_updated_at',
    invalidate_hard_deletes=True,
    tags=['snapshot', 'core']
) }}

select
    booking_id,
    partner_id_offer,
    booking_price,
    booking_currency,
    booking_status,
    booking_created_at,
    booking_updated_at,
    {{ var('dwh_loaddatetime') }} as dwh_load_time
from {{ ref('raw_bookings') }}

{% endsnapshot %}
