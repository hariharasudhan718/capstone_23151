{{ config(
    materialized='incremental',
    unique_key='supplier_id',
    incremental_strategy='merge'
) }}

{{ get_latest_records(
    source_table=source('raw', 'supplier_details'),
    unique_key='supplier_id',
    last_modified_column='last_modified_date'
) }}