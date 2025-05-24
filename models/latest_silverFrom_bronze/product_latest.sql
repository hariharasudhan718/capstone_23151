{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

{{ get_latest_records(
    source_table=source('raw', 'product_details'),
    unique_key='product_id',
    last_modified_column='last_modified_date'
) }}
