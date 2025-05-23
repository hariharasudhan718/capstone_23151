{{
    config(materialized='incremental', unique_key='supplier_id',constraints={
            'primary_key':['supplier_id']
        })
}}

SELECT 
    SUBSTRING(supplier_id, 4, LENGTH(supplier_id))::integer AS supplier_id,
    {{string_validation('supplier_name')}} AS supplier_name,
    {{string_validation('address')}} AS address,
    {{string_validation('category_supplied')}} AS category_supplied,
    {{string_validation('contact_person')}} AS contact_person,
    {{string_validation('contract_id')}} AS contract_id,
    lower({{string_validation('website')}}) AS website,
    {{email_validation('email')}} AS email,
    {{phone_number_validation('phone')}} AS phone,
    substring(tax_id,4,length(tax_id))::integer as tax_id,
    substring(payment_terms,5,length(payment_terms))::integer as payment_terms_net,
    {{string_validation('supplier_type')}} as supplier_type, 
    {{string_validation('preferred_carrier')}} AS preferred_carrier,
    credit_rating AS credit_rating,
    CASE 
        WHEN quality_rating = 'Excellent' THEN 5.0
        WHEN quality_rating = 'Good' THEN 4.4
        WHEN quality_rating = 'Satisfactory' THEN 3.9
        WHEN quality_rating = 'Fair' THEN 3.4
        WHEN quality_rating = 'Poor' THEN 2.9
        ELSE NULL
    END AS quality_rating,
    {{date_validation('contract_start_date')}} AS contract_start_date,
    {{date_validation('contract_end_date')}} AS contract_end_date,
    {{date_validation('last_modified_date')}} AS last_modified_date,
    try_to_date(last_order_date)::date AS last_order_date,
    case when year_established is null then 9999 else (year_established)::integer end as year_established,
    CASE WHEN average_delay_days IS NULL THEN 0 ELSE average_delay_days END AS average_delay_days,
    CASE WHEN defect_rate IS NULL THEN 0 ELSE defect_rate END AS defect_rate,
    CASE WHEN lead_time_days IS NULL THEN 0 ELSE lead_time_days END AS lead_time_days,
    CASE WHEN minimum_order_quantity IS NULL THEN 0 ELSE minimum_order_quantity END AS minimum_order_quantity,
    CASE WHEN on_time_delivery_rate IS NULL THEN 0 ELSE on_time_delivery_rate END AS on_time_delivery_rate,
    CASE WHEN response_time_hours IS NULL THEN 0 ELSE response_time_hours END AS response_time_hours,
    CASE WHEN returns_percentage IS NULL THEN 0 ELSE returns_percentage END AS returns_percentage,
    CASE WHEN is_active IS NULL THEN FALSE ELSE is_active END AS is_active,
    CASE WHEN exclusivity IS NULL THEN FALSE ELSE exclusivity END AS exclusivity,
    CASE WHEN renewal_option IS NULL THEN FALSE ELSE renewal_option END AS renewal_option,
    --(on_time_delivery_rate * 0.6) + ((100 - (average_delay_days * 10)) * 0.3) + (early_delivery_rate * 0.1) as calculated_timeliness_score,

FROM silver_layer.supplier_latest
