{{
    config(
        materialized = 'incremental',
        schema='gold_layer'
    )
}}
SELECT 
    md5(campaign_id || last_modified_date) as campaign_key, 
    campaign_id,
    target_audience,
    budget,
    duration_days,
    roi_calculation,
    start_date,
    end_date
from silver_layer.campaign_transformations
