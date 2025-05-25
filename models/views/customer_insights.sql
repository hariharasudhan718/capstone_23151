{{
    config(
        materialized = 'view'
    )
}}

WITH customer_lifetime_value AS (
    SELECT
        fs.customer_id,
        SUM(fs.total_sales_amount) AS LifetimeValue
    FROM
        gold_layer.fact_sales AS fs
    GROUP BY
        fs.customer_id
),

customer_purchases AS (
    SELECT
        fs.customer_id,
        COUNT(*) AS PurchaseCount
    FROM
        gold_layer.fact_sales AS fs
    GROUP BY
        fs.customer_id
),

repeat_purchase_rate AS (
    SELECT
        fs.customer_id,
        (COUNT(DISTINCT CASE WHEN cp.PurchaseCount > 1 THEN fs.customer_id END) * 100.0) /
        COUNT(DISTINCT fs.customer_id) AS RepeatPurchaseRate
    FROM
        gold_layer.fact_sales AS fs
    JOIN
        customer_purchases cp ON fs.customer_id = cp.customer_id
    GROUP BY
        fs.customer_id
),

customer_segmentation AS (
    SELECT
        fs.customer_id,
        dc.age_segment AS CustomerSegment,
        COUNT(DISTINCT fs.order_id) AS NumberOfOrders,
        SUM(fs.total_sales_amount) AS TotalSales,
        AVG(fs.total_sales_amount) AS AverageOrderValue
    FROM
        gold_layer.fact_sales AS fs
    JOIN
        gold_layer.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY
        fs.customer_id,
        dc.age_segment
)

SELECT distinct
    dc.customer_id AS CustomerID,
    dc.full_name AS CustomerName,
    clv.LifetimeValue,
    rpr.RepeatPurchaseRate,
    cs.CustomerSegment,
    cs.NumberOfOrders,
    cs.TotalSales,
    cs.AverageOrderValue
FROM
    gold_layer.dim_customer dc
JOIN
    customer_lifetime_value clv ON dc.customer_id = clv.customer_id
JOIN
    repeat_purchase_rate rpr ON dc.customer_id = rpr.customer_id
JOIN
    customer_segmentation cs ON dc.customer_id = cs.customer_id
ORDER BY
    clv.LifetimeValue DESC
