{{ config(
    materialized='incremental',
    unique_key='campaign_id',
    incremental_strategy='merge'
) }}

{{ get_latest_records(
    source_table=source('raw', 'campaign_details'),
    unique_key='campaign_id',
    last_modified_column='last_modified_date'
) }}