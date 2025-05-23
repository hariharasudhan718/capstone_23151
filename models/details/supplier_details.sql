-- models/supplier_details.sql
{{
  config(
    materialized = 'incremental',
    unique_key='supplier_id',
    schema = 'raw_bronze'
  )
}}
WITH source_data AS (
    SELECT DISTINCT
        e.value:supplier_id::STRING AS supplier_id,
        e.value:supplier_name::STRING AS supplier_name,
        e.value:contact_information:contact_person::STRING AS contact_person,
        e.value:contact_information:email::STRING AS email,
        e.value:contact_information:phone::STRING AS phone,
        e.value:contact_information:address::STRING AS address,
        e.value:payment_terms::STRING AS payment_terms,
        e.value:supplier_type::STRING AS supplier_type,
        c.value::STRING AS category_supplied,
        e.value:contract_details:contract_id::STRING AS contract_id,
        e.value:contract_details:start_date::STRING AS contract_start_date,
        e.value:contract_details:end_date::STRING AS contract_end_date,
        e.value:contract_details:renewal_option::BOOLEAN AS renewal_option,
        e.value:contract_details:exclusivity::BOOLEAN AS exclusivity,
        e.value:performance_metrics:on_time_delivery_rate::FLOAT AS on_time_delivery_rate,
        e.value:performance_metrics:average_delay_days::FLOAT AS average_delay_days,
        e.value:performance_metrics:defect_rate::FLOAT AS defect_rate,
        e.value:performance_metrics:returns_percentage::FLOAT AS returns_percentage,
        e.value:performance_metrics:quality_rating::STRING AS quality_rating,
        e.value:performance_metrics:response_time_hours::FLOAT AS response_time_hours,
        e.value:lead_time_days::INT AS lead_time_days,
        e.value:minimum_order_quantity::INT AS minimum_order_quantity,
        e.value:preferred_carrier::STRING AS preferred_carrier,
        e.value:credit_rating::STRING AS credit_rating,
        e.value:tax_id::STRING AS tax_id,
        e.value:year_established::INT AS year_established,
        e.value:website::STRING AS website,
        e.value:last_order_date::DATE AS last_order_date,
        e.value:is_active::BOOLEAN AS is_active,
        e.value:last_modified_date::STRING AS last_modified_date
    FROM {{ source('raw_external', 'raw_supplier_data_bronze') }},
         LATERAL FLATTEN(input => json_data:suppliers_data) AS e,
         LATERAL FLATTEN(input => e.value:categories_supplied) AS c
)
SELECT * FROM source_data
