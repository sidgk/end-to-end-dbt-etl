{% snapshot currency_snapshot %}

{{
    config(
      target_schema='omio_snapshots',
      unique_key='currency_code',
      strategy='timestamp',
      updated_at='date',
      tags=['snapshot', 'core'],
      enabled=true
    )
}}


select
  date,
  currency_code,
  rate_to_eur
from 
  {{ ref('raw_currency') }}

{% endsnapshot %}
