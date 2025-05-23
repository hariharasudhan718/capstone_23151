{{
    config(
        materialized = 'incremental',
        schema='gold_layer'
    )
}}
SELECT 
    md5(p.product_id || p.last_modified_date) as product_key, 
    p.product_id as product_id,
    p.name as product_name,
    p.category as category,
    p.subcategory as subcategory,
    p.brand,
    p.color as color,
    p.size as size,
    p.unit_price as unit_price,
    p.cost_price as cost_price,
    s.supplier_name as supplier_name,
    s.contact_person as supplier_contact_person,
    s.email as supplier_email,
    s.phone as supplier_phone
from 
    silver_layer.product_transformations p left join silver_layer.supplier_transformations s on p.supplier_id = s.supplier_id