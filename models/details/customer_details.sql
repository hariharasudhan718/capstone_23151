{{
  config(
    materialized = 'incremental',
    unique_key='customer_id',
    schema = 'raw_bronze',
    incremental_strategy='merge'

  )
}}
WITH source_data AS (
    SELECT DISTINCT
        e.value:customer_id::STRING AS customer_id,
        e.value:first_name::STRING AS first_name,
        e.value:last_name::STRING AS last_name,
        e.value:email::STRING AS email,
        e.value:phone::STRING AS phone,
        e.value:birth_date::STRING AS birth_date,
        e.value:address:street::STRING AS street,
        e.value:address:city::STRING AS city,
        e.value:address:state::STRING AS state,
        e.value:address:zip_code::STRING AS zip_code,
        e.value:address:country::STRING AS country,
        e.value:registration_date::STRING AS registration_date,
        e.value:preferred_communication::STRING AS preferred_communication,
        e.value:occupation::STRING AS occupation,
        e.value:income_bracket::STRING AS income_bracket,
        e.value:loyalty_tier::STRING AS loyalty_tier,
        e.value:total_purchases::INT AS total_purchases,
        e.value:total_spend::FLOAT AS total_spend,
        e.value:preferred_payment_method::STRING AS preferred_payment_method,
        e.value:marketing_opt_in::BOOLEAN AS marketing_opt_in,
        e.value:last_purchase_date::STRING AS last_purchase_date,
        e.value:last_modified_date::STRING AS last_modified_date
    FROM {{ source('raw_external', 'raw_customer_data_bronze') }},
         LATERAL FLATTEN(input => json_data:customers_data) AS e
)
SELECT * FROM source_data
{% if is_incremental() %}
  WHERE last_modified_date > (SELECT MAX(last_modified_date) FROM {{ this }})
{% endif %}
