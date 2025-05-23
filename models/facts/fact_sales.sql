{{
  config(
    materialized = 'incremental',
    schema = 'gold_layer'
  )
}}

SELECT 
    md5(cu.customer_id || o.order_id || p.product_id || st.store_id || ca.campaign_id) AS sales_key,
    o.order_id AS order_id,
    cu.customer_id as customer_id,
    cu.customer_key AS customer_key,
    p.product_key AS product_key,
    st.store_key AS store_key,
    d.datekey AS date_key,
    e.employee_key AS employee_key,
    ca.campaign_key AS campaign_key,
    o.quantity AS quantity_sold,
    o.unit_price AS unit_price,
    o.quantity * o.unit_price AS total_sales_amount,
    o.quantity * p.cost_price AS cost_amount,
    o.quantity * o.unit_price - o.quantity * p.cost_price - o.discount_amount - o.shipping_cost AS profit_amount,
    o.total_discount AS discount_amount,
    o.shipping_cost AS shipping_cost,
    st.region AS region,
    CASE WHEN o.order_source = 'In-store' THEN 'In-store' ELSE 'Online' END AS sales_channel,
    cu.age_segment AS customer_segment
FROM 
    silver_layer.order_transformations o 
JOIN 
    silver_layer_gold_layer.dim_customer cu ON o.customer_id = cu.customer_id
JOIN
    silver_layer_gold_layer.dim_marketing_campaign ca ON ca.campaign_id = o.campaign_id
JOIN 
    silver_layer_gold_layer.dim_employee e ON e.employee_id = o.employee_id
JOIN 
    silver_layer_gold_layer.dim_product p ON p.product_id = o.product_id
JOIN 
    silver_layer_gold_layer.dim_store st ON st.store_id = o.store_id
JOIN
    silver_layer_gold_layer.dim_date d ON DATE_TRUNC('day', o.order_date) = d.FullDate