{{
    config(
        materialized = 'table',
        schema='gold_layer'
    )
}}
SELECT 
    md5(store_id || last_modified_date) as store_key, 
    store_id,
    store_name,
    city,
    country,
    region,
    state,
    street,
    zip_code,
    store_type,
    opening_date,
    store_size_category
from silver_layer.store_transformations