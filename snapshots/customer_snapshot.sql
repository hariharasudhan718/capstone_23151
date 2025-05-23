{% snapshot customer_snapshot %}
    {{
        config(
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='timestamp',
            updated_at='last_modified_date'
        )
    }}

    select * from {{ source('gold_src', 'dim_customer') }}
 {% endsnapshot %}