-- models/campaign_details.sql
{{
  config(
    materialized = 'incremental',
    unique_key='campaign_id',
    schema = 'raw_bronze'
  )
}}
WITH source_data AS (
    SELECT DISTINCT
        e.value:campaign_id::STRING AS campaign_id,
        e.value:campaign_name::STRING AS campaign_name,
        e.value:start_date::STRING AS start_date,
        e.value:end_date::STRING AS end_date,
        e.value:budget::STRING AS budget,
        e.value:target_audience::STRING AS target_audience,
        e.value:campaign_type::STRING AS campaign_type,
        e.value:channel::STRING AS channel,
        e.value:total_cost::STRING AS total_cost,
        e.value:total_revenue::STRING AS total_revenue,
        e.value:roi_calculation::FLOAT AS roi_calculation,
        e.value:description::STRING AS description,
        e.value:last_modified_date::STRING AS last_modified_date
    FROM {{ source('raw_external', 'raw_campaign_data_bronze') }},
         LATERAL FLATTEN(input => json_data:campaigns_data) AS e
)
SELECT * FROM source_data
