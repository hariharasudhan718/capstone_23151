{{
    config(
        materialized = 'table',
        schema='gold_layer'
    )
}}
SELECT 
    md5(supplier_id || last_modified_date) as supplier_key, 
    supplier_id,
    supplier_name,
    contact_person,
    email,
    phone,
    website,
    payment_terms_net as payment_terms,
    supplier_type,
    on_time_delivery_rate,
    quality_rating,
    is_active as active_contract_details
from silver_layer.supplier_transformations