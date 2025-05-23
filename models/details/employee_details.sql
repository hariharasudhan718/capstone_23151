{{
  config(
    materialized = 'incremental',
    unique_key='employee_id',
    schema = 'raw_bronze'
  )
}}
WITH source_data AS (
    SELECT DISTINCT
        e.value:employee_id::STRING AS employee_id,
        e.value:first_name::STRING AS first_name,
        e.value:last_name::STRING AS last_name,
        e.value:email::STRING AS email,
        e.value:phone::STRING AS phone,
        e.value:hire_date::STRING AS hire_date,
        e.value:role::STRING AS role,
        e.value:department::STRING AS department,
        e.value:work_location::STRING AS work_location,
        e.value:salary::FLOAT AS salary,
        e.value:manager_id::STRING AS manager_id,
        e.value:employment_status::STRING AS employment_status,
        e.value:education::STRING AS education,
        e.value:address:street::STRING AS street,
        e.value:address:city::STRING AS city,
        e.value:address:state::STRING AS state,
        e.value:address:zip_code::STRING AS zip_code,
        e.value:date_of_birth::STRING AS date_of_birth,
        e.value:sales_target::FLOAT AS sales_target,
        e.value:current_sales::FLOAT AS current_sales,
        e.value:performance_rating::FLOAT AS performance_rating,
        c.value::STRING AS certification,
        e.value:last_modified_date::STRING AS last_modified_date
    FROM {{ source('raw_external', 'raw_employee_data_bronze') }},
         LATERAL FLATTEN(input => json_data:employees_data) AS e,
         LATERAL FLATTEN(input => e.value:certifications) AS c
)
SELECT * FROM source_data
