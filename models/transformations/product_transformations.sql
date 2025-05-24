-- Categorize products hierarchically (category > subcategory > product_line)
-- Normalize product names and categories (Pascal Case)
{{ config(
    materialized='incremental',
    unique_key='product_id',
    constraints={
        'primary_key': ['product_id']
    },
    incremental_strategy='merge'
) }}
select 
    SUBSTRING(product_id, 5, LENGTH(product_id))::integer AS product_id,
    {{string_validation('name')}} as name,
    {{string_validation('short_description')}} as short_description,
    {{string_validation('technical_specs')}} as technical_specs,
    {{string_validation('category')}} as category,
    {{string_validation('subcategory')}} as subcategory,
    {{string_validation('product_line')}} as product_line,
    {{string_validation('brand')}} as brand,
    {{string_validation('color')}} as color,
    {{string_validation('size')}} as size,
    unit_price::integer as unit_price,
    cost_price::integer as cost_price,
    SUBSTRING(supplier_id, 5, LENGTH(supplier_id))::integer AS supplier_id,
    case when stock_quantity is null then 0 else stock_quantity::integer end as stock_quantity,
    case when reorder_level is null then 0 else reorder_level::integer end as reorder_level,
    case when weight is null then 0 else substring(weight,1,length(weight)-3)::float end as weight,
    split_part(dimensions,' x ',1)::integer as height_cm,
    split_part(dimensions,' x ',2)::integer as length_cm,
    substring(split_part(dimensions,' x ',3),1,length(split_part(dimensions,' x ',3))-3)::integer as breadth_cm,
    is_featured,
    {{ date_validation('launch_date') }} as launch_date,
    case when warranty_period='No warranty' then 0 else substring(warranty_period,1,1)::integer end as warranty_period_years,
    {{date_validation('last_modified_date')}} as last_modified_date,
    name || ' ' || short_description || ' ' || technical_specs as full_description,
    round((unit_price-cost_price)*100/unit_price,2) as profit_margin_percentage,
    case when stock_quantity::integer < reorder_level::integer then 1 else 0 end as low_stock_flag
from silver_layer.product_latest


