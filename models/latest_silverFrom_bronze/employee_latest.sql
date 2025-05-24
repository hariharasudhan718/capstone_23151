{{ config(
    materialized='incremental',
    unique_key='employee_id',
    incremental_strategy='merge'
) }}

{{ get_latest_records(
    source_table=source('raw', 'employee_details'),
    unique_key='employee_id',
    last_modified_column='last_modified_date'
) }}