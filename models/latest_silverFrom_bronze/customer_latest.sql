{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

{{ get_latest_records(
    source_table=source('raw', 'customer_details'),
    unique_key='customer_id',
    last_modified_column='last_modified_date'
) }}
