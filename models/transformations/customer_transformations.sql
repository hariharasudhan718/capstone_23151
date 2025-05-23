-- Constraints to be added
-- Phone number formats
-- Email formats when in wrong format NA is given
-- zip codes
-- Null birth dates have been replaced with random date: '9999/12/31'
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        constraints={
            'primary_key':['customer_id']
        }              
    )
}}
SELECT 
    SUBSTRING(customer_id, 5, LENGTH(customer_id))::integer AS customer_id,
    {{ date_validation('birth_date') }} AS birth_date,
    {{ string_validation('city') }} AS city,
    upper({{ string_validation('country') }}) AS country,
    {{ email_validation('email') }} AS email,
    {{ string_validation('first_name') }} AS first_name,
    {{ string_validation('income_bracket') }} AS income_bracket,
    {{ date_validation('last_modified_date') }} AS last_modified_date,
    {{ string_validation('last_name') }} AS last_name,
    {{ date_validation('last_purchase_date') }} AS last_purchase_date,
    {{ string_validation('loyalty_tier') }} AS loyalty_tier,
    CASE
        WHEN marketing_opt_in IS NULL THEN 0
        ELSE marketing_opt_in
    END AS marketing_opt_in,
    {{ string_validation('occupation') }} AS occupation,
    {{ phone_number_validation('phone') }} AS phone,
    {{ string_validation('preferred_communication') }} AS preferred_communication,
    {{ string_validation('preferred_payment_method') }} AS preferred_payment_method,
    {{ date_validation('registration_date') }} AS registration_date,
    upper({{ string_validation('state') }}) AS state,
    {{ string_validation('street') }} AS street,
    CASE 
        WHEN total_purchases IS NULL THEN 0 
        ELSE total_purchases 
    END AS total_purchases,
    CASE 
        WHEN total_spend IS NULL THEN 0 
        ELSE total_spend 
    END AS total_spend,
    {{ string_validation('zip_code') }} AS zip_code,
    first_name || ' ' || last_name AS full_name,
    CASE 
        WHEN birth_date IS NULL THEN 0
        ELSE FLOOR(DATEDIFF(year, 
            COALESCE(
                TRY_TO_DATE(birth_date, 'YYYY/MM/DD'),
                TRY_TO_DATE(birth_date, 'YYYY-MM-DD'),
                TRY_TO_DATE(birth_date, 'DD/MM/YYYY'),
                TRY_TO_DATE(birth_date, 'DD-MM-YYYY'),
                TRY_TO_DATE(birth_date, 'MM-DD-YYYY'),
                TRY_TO_DATE(birth_date, 'MM/DD/YYYY'),
                TRY_TO_DATE(birth_date, 'YYYY/DD/MM'),
                TRY_TO_DATE(birth_date, 'YYYY-DD-MM')
            ), 
            CURRENT_DATE
        ))
    END AS age,
    CASE
        WHEN FLOOR(DATEDIFF(year, 
            COALESCE(
                TRY_TO_DATE(birth_date, 'YYYY/MM/DD'),
                TRY_TO_DATE(birth_date, 'YYYY-MM-DD'),
                TRY_TO_DATE(birth_date, 'DD/MM/YYYY'),
                TRY_TO_DATE(birth_date, 'DD-MM-YYYY'),
                TRY_TO_DATE(birth_date, 'MM-DD-YYYY'),
                TRY_TO_DATE(birth_date, 'MM/DD/YYYY'),
                TRY_TO_DATE(birth_date, 'YYYY/DD/MM'),
                TRY_TO_DATE(birth_date, 'YYYY-DD-MM')
            ), 
            CURRENT_DATE
        )) < 18 THEN 'Under 18'
        WHEN FLOOR(DATEDIFF(year, 
            COALESCE(
                TRY_TO_DATE(birth_date, 'YYYY/MM/DD'),
                TRY_TO_DATE(birth_date, 'YYYY-MM-DD'),
                TRY_TO_DATE(birth_date, 'DD/MM/YYYY'),
                TRY_TO_DATE(birth_date, 'DD-MM-YYYY'),
                TRY_TO_DATE(birth_date, 'MM-DD-YYYY'),
                TRY_TO_DATE(birth_date, 'MM/DD/YYYY'),
                TRY_TO_DATE(birth_date, 'YYYY/DD/MM'),
                TRY_TO_DATE(birth_date, 'YYYY-DD-MM')
            ), 
            CURRENT_DATE
        )) BETWEEN 18 AND 35 THEN 'Young'
        WHEN FLOOR(DATEDIFF(year, 
            COALESCE(
                TRY_TO_DATE(birth_date, 'YYYY/MM/DD'),
                TRY_TO_DATE(birth_date, 'YYYY-MM-DD'),
                TRY_TO_DATE(birth_date, 'DD/MM/YYYY'),
                TRY_TO_DATE(birth_date, 'DD-MM-YYYY'),
                TRY_TO_DATE(birth_date, 'MM-DD-YYYY'),
                TRY_TO_DATE(birth_date, 'MM/DD/YYYY'),
                TRY_TO_DATE(birth_date, 'YYYY/DD/MM'),
                TRY_TO_DATE(birth_date, 'YYYY-DD-MM')
            ), 
            CURRENT_DATE
        )) BETWEEN 36 AND 55 THEN 'Middle-aged'
        ELSE 'Senior'
    END AS age_segment,
    {{string_validation('street')}} || ' ' || {{string_validation('city')}} ||' '|| {{string_validation('state')}}||' ' || {{string_validation('country')}}||' ' || {{string_validation('zip_code')}} AS address
FROM 
    silver_layer.customer_latest
