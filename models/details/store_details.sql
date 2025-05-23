-- models/store_details.sql
{{
  config(
    materialized = 'incremental',
    unique_key='store_id',
    schema = 'raw_bronze'
  )
}}
WITH source_data AS (
    SELECT DISTINCT
        e.value:store_id::STRING AS store_id,
        e.value:store_name::STRING AS store_name,
        e.value:address:street::STRING AS street,
        e.value:address:city::STRING AS city,
        e.value:address:state::STRING AS state,
        e.value:address:zip_code::STRING AS zip_code,
        e.value:address:country::STRING AS country,
        e.value:region::STRING AS region,
        e.value:store_type::STRING AS store_type,
        e.value:opening_date::STRING AS opening_date,
        e.value:size_sq_ft::INT AS size_sq_ft,
        e.value:manager_id::STRING AS manager_id,
        e.value:phone_number::STRING AS phone_number,
        e.value:email::STRING AS email,
        e.value:operating_hours:weekdays::STRING AS weekdays_hours,
        e.value:operating_hours:weekends::STRING AS weekends_hours,
        e.value:operating_hours:holidays::STRING AS holidays_hours,
        s.value::STRING AS service,
        e.value:employee_count::INT AS employee_count,
        e.value:is_active::BOOLEAN AS is_active,
        e.value:monthly_rent::FLOAT AS monthly_rent,
        e.value:sales_target::FLOAT AS sales_target,
        e.value:current_sales::FLOAT AS current_sales,
        e.value:last_modified_date::STRING AS last_modified_date
    FROM {{ source('raw_external', 'raw_store_data_bronze') }},
         LATERAL FLATTEN(input => json_data:stores_data) AS e,
         LATERAL FLATTEN(input => e.value:services) AS s
)
SELECT * FROM source_data
