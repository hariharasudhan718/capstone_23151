{{
    config(
        materialized = 'table',
        schema='gold_layer',
    )
}}
SELECT 
    md5(customer_id || last_modified_date) as customer_key, 
    customer_id,
    full_name,
    email,
    phone,
    city,
    country,
    state,
    street,
    zip_code,
    age,
    birth_date,
    INCOME_BRACKET,
    OCCUPATION,
    age_segment,
    registration_date,
    last_modified_date
from silver_layer.customer_transformations
    
