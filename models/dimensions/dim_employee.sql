{{
    config(
        materialized = 'table',
        schema='gold_layer'
    )
}}
SELECT 
    md5(employee_id || last_modified_date) as employee_key, 
    employee_id,
    full_name,
    role,
    work_location,
    tenure_years,
    email,
    phone,
    target_achievement_percentage,
    performance_rating
from silver_layer.employee_transformations
