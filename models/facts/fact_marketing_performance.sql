{{
  config(
    materialized = 'incremental',
    schema = 'gold_layer'
  )
}}

SELECT
    fs.campaign_key,
    fs.date_key,
    fs.customer_key,
    SUM(fs.total_sales_amount) AS TotalSalesInfluenced,
    COUNT(DISTINCT fs.customer_key) AS NewCustomersAcquired,
    (COUNT(DISTINCT fs.customer_key) * 100) / COUNT(DISTINCT fs.customer_key) AS RepeatPurchaseRate,
    (sum(fs.total_sales_amount) - c.budget) / c.budget * 100 AS ROIMetrics
FROM
    {{ ref('fact_sales') }} fs
JOIN
    {{ ref('dim_marketing_campaign') }} c ON fs.campaign_key = c.campaign_key
join 
    silver_layer.order_transformations o on fs.order_id=o.order_id
WHERE
    o.order_date BETWEEN c.start_date AND c.end_date
GROUP BY
    fs.campaign_key, fs.date_key, fs.customer_key, c.budget
