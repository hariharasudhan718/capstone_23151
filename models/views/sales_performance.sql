{{
    config(
        materialized = 'view'
    )
}}

WITH category_sales AS (
    SELECT
        SUM(fs.total_sales_amount) AS total_sales_amount,
        dp.category
    FROM
        {{ ref('fact_sales') }} fs
    JOIN
        {{ ref('dim_product') }} dp ON fs.product_key = dp.product_key
    GROUP BY
        dp.category
),

region_sales AS (
    SELECT
        SUM(fs.total_sales_amount) AS total_sales_amount,
        fs.region
    FROM
        {{ ref('fact_sales') }} fs
    GROUP BY
        fs.region
),

product_sales AS (
    SELECT
        SUM(fs.quantity_sold) AS total_quantity_sold,
        dp.product_name
    FROM
        {{ ref('fact_sales') }} fs
    JOIN
        {{ ref('dim_product') }} dp ON fs.product_key = dp.product_key
    GROUP BY
        dp.product_name
    ORDER BY
        total_quantity_sold DESC
),

segment_sales AS (
    SELECT
        SUM(fs.total_sales_amount) AS total_sales_amount,
        fs.customer_segment
    FROM
        {{ ref('fact_sales') }} fs
    GROUP BY
        fs.customer_segment
)

SELECT
    cs.total_sales_amount AS category_sales_amount,
    cs.category,
    rs.total_sales_amount AS region_sales_amount,
    rs.region,
    ps.total_quantity_sold,
    ps.product_name,
    ss.total_sales_amount AS segment_sales_amount,
    ss.customer_segment
FROM
    category_sales cs,
    region_sales rs,
    product_sales ps,
    segment_sales ss
