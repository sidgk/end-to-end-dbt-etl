{% snapshot passenger_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='passenger_id',
    strategy='timestamp',
    updated_at='dwh_loaddatetime',
    invalidate_hard_deletes=True,
    tags=['snapshot', 'core']
) }}

select
    passenger_id,
    booking_id,
    first_name,
    last_name,
    passenger_type,
    age,
    dwh_loaddatetime
from {{ ref('raw_passangers') }}

{% endsnapshot %}
