--•	Aggregate order items 
--Sample Calculation Logic:
--SELECT order_id, COUNT(product_id) AS total_items, SUM(quantity) AS total_quantity, SUM(quantity * unit_price) AS total_amount, SUM(quantity * cost_price) AS total_cost, SUM(discount_amount) AS total_discount FROM order_items GROUP BY order_id

--•	Standardize role descriptions 
--(e.g., Sales Associate → Associate
--Senior Manager → Senior Manager
--Store Manager → Manager

--Orders_Processed  = Total number of orders processed by Employee
--Total_Sales_Amount = Sum to amount from orders data by Employee



{{
    config(materialized='incremental',unique_key='customer_id',constraints={
            'primary_key':['order_id']
        },incremental_strategy='merge' )
}}
SELECT 
    SUBSTRING(order_id, 4, LENGTH(order_id))::integer AS order_id,
    SUBSTRING(customer_id, 5, LENGTH(customer_id))::integer AS customer_id,
    order_date::timestamp as order_date,
    case when total_amount is null then 0 else total_amount::float end as total_amount,
    case when discount_amount is null then 0 else discount_amount::float end as discount_amount,
    case when shipping_cost is null then 0 else shipping_cost::float end as shipping_cost,
    case when tax_amount is null then 0 else tax_amount::float end as tax_amount,
    {{string_validation('order_status')}} as order_status,
    {{string_validation('payment_method')}} as payment_method,
    {{string_validation('shipping_method')}} as shipping_method,
    {{string_validation('shipping_street')}} as shipping_street,
    {{string_validation('shipping_city')}} as shipping_city,
    {{string_validation('shipping_state')}} as shipping_state,
    {{string_validation('shipping_zip_code')}} as shipping_zip_code,
    {{string_validation('billing_street')}} as billing_street,
    {{string_validation('billing_city')}} as billing_city,
    {{string_validation('billing_state')}} as billing_state,
    {{string_validation('billing_zip_code')}} as billing_zip_code,
    SUBSTRING(product_id, 5, LENGTH(product_id))::integer AS product_id,
    case when quantity is null then 0 else quantity end as quantity,
    case when unit_price is null then 0 else unit_price end as unit_price,
    case when cost_price is null then 0 else cost_price end as cost_price,
    case when item_discount_amount is null then 0 else item_discount_amount end as item_discount_amount,
    SUBSTRING(store_id, 6, LENGTH(store_id))::integer AS store_id,
    SUBSTRING(employee_id, 4, LENGTH(employee_id))::integer AS employee_id,
    shipping_date::timestamp as shipping_date,
    delivery_date::timestamp as delivery_date,
    estimated_delivery_date::timestamp as estimated_delivery_date,
    {{ string_validation('order_source') }} as order_source,
    SUBSTRING(campaign_id, 5, LENGTH(campaign_id))::integer AS campaign_id,
    created_at::timestamp as created_at,
    total_amount-(quantity*cost_price)-discount_amount-shipping_cost-tax_amount as profit_amount,
    (total_amount-quantity*cost_price-discount_amount)/total_amount as profit_margin_percentage,
    CASE
        WHEN EXTRACT(HOUR FROM order_date::timestamp) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM order_date::timestamp) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM order_date::timestamp) BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS order_time_of_day,
    extract(week from order_date::timestamp) as week_of_order,
    extract(month from order_date::timestamp) as month_of_order,
    extract(quarter from order_date::timestamp) as quarter_of_order,
    extract(year from order_date::timestamp) as year_of_order,
    DATEDIFF(day,order_date,shipping_date) as processing_days,
    datediff(day,shipping_date,delivery_date) as shipping_days,
    CASE 
        WHEN delivery_date IS NOT NULL AND delivery_date <= estimated_delivery_date THEN 'On Time' 
        WHEN delivery_date IS NOT NULL AND delivery_date > estimated_delivery_date THEN 'Delayed' 
        WHEN delivery_date IS NULL AND CURRENT_DATE > estimated_delivery_date THEN 'Potentially Delayed' 
        ELSE 'In Transit' 
	END AS delivery_status,
    COUNT(product_id) OVER (PARTITION BY order_id) AS total_items,
    SUM(quantity) OVER (PARTITION BY order_id) AS total_quantity,
    SUM(quantity * unit_price) OVER (PARTITION BY order_id) AS total__sales_amount,
    SUM(quantity * cost_price) OVER (PARTITION BY order_id) AS total_cost,
    SUM(discount_amount) OVER (PARTITION BY order_id) AS total_discount
from silver_layer.order_latest
   