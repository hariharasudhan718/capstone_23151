{{
    config(materialized='incremental',unique_key='employee_id',constraints={
            'primary_key':['employee_id']
        })
}}
select 
    SUBSTRING(employee_id, 4, LENGTH(employee_id))::integer AS employee_id,
    {{string_validation('first_name')}} as first_name,
    {{string_validation('last_name')}} as last_name,
    {{email_validation('email')}} as email,
    {{phone_number_validation('phone')}} as phone,
    {{date_validation('hire_date')}} as hire_date,
    {{string_validation('role')}} as role,
    {{string_validation('department')}} as department,
    SUBSTRING(work_location, 6, LENGTH(work_location))::integer AS work_location,
    case when salary is null then 0 else salary end as salary,
    SUBSTRING(manager_id, 4, LENGTH(manager_id))::integer AS manager_id,
    {{string_validation('employment_status')}} as employment_status,
    {{string_validation('education')}} as education,
    {{string_validation('street')}} as street,
    {{string_validation('city')}} as city,
    {{string_validation('state')}} as state,
    case when zip_code is null then '' else zip_code end as zip_code,
    {{date_validation('date_of_birth')}} as date_of_birth,
    case when sales_target is null then 0 else sales_target end as sales_target,
    case when current_sales is null then 0 else current_sales end as current_sales,
    case when performance_rating is null then 0 else performance_rating end as performance_rating,
    {{string_validation('certification')}} as certification,
    {{date_validation('last_modified_date')}} as last_modified_date,
    first_name || ' ' || last_name as full_name,
    datediff(year,hire_date,current_date()) as tenure_years,
    round(current_sales*100/sales_target,2) as target_achievement_percentage
from silver_layer.employee_latest


