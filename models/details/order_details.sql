-- models/order_details.sql
{{
  config(
    materialized = 'incremental',
    unique_key='order_id',
    schema = 'raw_bronze',
    
incremental_strategy='merge'

  )
}}
WITH source_data AS (
    SELECT DISTINCT
        e.value:order_id::STRING AS order_id,
        e.value:customer_id::STRING AS customer_id,
        e.value:order_date::STRING AS order_date,
        e.value:total_amount::FLOAT AS total_amount,
        e.value:discount_amount::FLOAT AS discount_amount,
        e.value:shipping_cost::FLOAT AS shipping_cost,
        e.value:tax_amount::FLOAT AS tax_amount,
        e.value:order_status::STRING AS order_status,
        e.value:payment_method::STRING AS payment_method,
        e.value:shipping_method::STRING AS shipping_method,
        e.value:shipping_address:street::STRING AS shipping_street,
        e.value:shipping_address:city::STRING AS shipping_city,
        e.value:shipping_address:state::STRING AS shipping_state,
        e.value:shipping_address:zip_code::STRING AS shipping_zip_code,
        e.value:billing_address:street::STRING AS billing_street,
        e.value:billing_address:city::STRING AS billing_city,
        e.value:billing_address:state::STRING AS billing_state,
        e.value:billing_address:zip_code::STRING AS billing_zip_code,
        i.value:product_id::STRING AS product_id,
        i.value:quantity::INT AS quantity,
        i.value:unit_price::FLOAT AS unit_price,
        i.value:cost_price::FLOAT AS cost_price,
        i.value:discount_amount::FLOAT AS item_discount_amount,
        e.value:store_id::STRING AS store_id,
        e.value:employee_id::STRING AS employee_id,
        e.value:shipping_date::STRING AS shipping_date,
        e.value:delivery_date::STRING AS delivery_date,
        e.value:estimated_delivery_date::STRING AS estimated_delivery_date,
        e.value:order_source::STRING AS order_source,
        e.value:campaign_id::STRING AS campaign_id,
        e.value:created_at::STRING AS created_at
    FROM {{ source('raw_external', 'raw_order_data_bronze') }},
         LATERAL FLATTEN(input => json_data:orders_data) AS e,
         LATERAL FLATTEN(input => e.value:order_items) AS i
)
SELECT * FROM source_data
