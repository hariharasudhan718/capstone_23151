{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge'
) }}

{{ get_latest_records(
    source_table=source('raw', 'store_details'),
    unique_key='store_id',
    last_modified_column='last_modified_date'
) }}