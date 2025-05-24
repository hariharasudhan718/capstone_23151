{{
    config(materialized='incremental', unique_key='campaign_id',constraints={'primary_key':['campaign_id']},incremental_strategy='merge')
}}

SELECT 
    SUBSTRING(campaign_id, 5, LENGTH(campaign_id))::integer AS campaign_id,
    {{string_validation('campaign_name')}} AS campaign_name,
    {{string_validation('campaign_type')}} AS campaign_type,
    {{string_validation('channel')}} AS channel,
    {{string_validation('description')}} AS description,
    start_date::timestamp AS start_date,
    end_date::timestamp AS end_date,
    {{date_validation('last_modified_date')}} AS last_modified_date,
    CASE WHEN budget IS NULL THEN 0 ELSE replace(substring(budget,2,length(budget)),',','')::float END AS budget,
    CASE WHEN roi_calculation IS NULL THEN 0 ELSE roi_calculation END AS roi_calculation,
    CASE WHEN total_cost IS NULL THEN 0 ELSE replace(substring(total_cost,2,length(total_cost)),',','')::float END AS total_cost,
    CASE WHEN total_revenue IS NULL THEN 0 ELSE replace(substring(total_revenue,2,length(total_revenue)),',','')::float END AS total_revenue,
    {{string_validation('target_audience')}} AS target_audience,
    DATEDIFF(day,start_date,end_date) as duration_days
FROM silver_layer.campaign_latest
