-- models/product_details.sql
{{
  config(
    materialized = 'incremental',
    unique_key='product_id',
    schema = 'raw_bronze'
  )
}}
WITH source_data AS (
    SELECT DISTINCT
        p.value:product_id::STRING AS product_id,
        p.value:name::STRING AS name,
        p.value:short_description::STRING AS short_description,
        p.value:technical_specs::STRING AS technical_specs,
        p.value:category::STRING AS category,
        p.value:subcategory::STRING AS subcategory,
        p.value:product_line::STRING AS product_line,
        p.value:brand::STRING AS brand,
        p.value:color::STRING AS color,
        p.value:size::STRING AS size,
        p.value:unit_price::FLOAT AS unit_price,
        p.value:cost_price::FLOAT AS cost_price,
        p.value:supplier_id::STRING AS supplier_id,
        p.value:stock_quantity::INT AS stock_quantity,
        p.value:reorder_level::INT AS reorder_level,
        p.value:weight::STRING AS weight,
        p.value:dimensions::STRING AS dimensions,
        p.value:is_featured::BOOLEAN AS is_featured,
        p.value:launch_date::STRING AS launch_date,
        p.value:warranty_period::STRING AS warranty_period,
        p.value:last_modified_date::STRING AS last_modified_date
    FROM {{ source('raw_external', 'raw_product_data_bronze') }},
         LATERAL FLATTEN(input => json_data:products_data) AS p
)
SELECT * FROM source_data
