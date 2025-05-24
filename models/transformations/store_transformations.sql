{{
    config(materialized='incremental', unique_key='store_id',constraints={
            'primary_key':['store_id']},incremental_strategy='merge')
}}
SELECT 
    SUBSTRING(store_id, 6, LENGTH(store_id))::integer AS store_id,
    {{string_validation('store_name')}} AS store_name,
    {{string_validation('store_type')}} AS store_type,
    {{string_validation('street')}} AS street,
    {{string_validation('city')}} AS city,
    {{string_validation('state')}} AS state,
    {{string_validation('country')}} AS country,
    {{string_validation('region')}} AS region,
    {{string_validation('service')}} AS service,
    {{email_validation('email')}} AS email,
    {{phone_number_validation('phone_number')}} AS phone_number,
    SUBSTRING(manager_id, 4, LENGTH(manager_id))::integer AS manager_id,
    {{date_validation('opening_date')}} AS opening_date,
    {{date_validation('last_modified_date')}} AS last_modified_date,
    CASE WHEN current_sales IS NULL THEN 0 ELSE current_sales END AS current_sales,
    CASE WHEN sales_target IS NULL THEN 0 ELSE sales_target END AS sales_target,
    CASE WHEN monthly_rent IS NULL THEN 0 ELSE monthly_rent END AS monthly_rent,
    CASE WHEN employee_count IS NULL THEN 0 ELSE employee_count END AS employee_count,
    CASE WHEN size_sq_ft IS NULL THEN 0 ELSE size_sq_ft END AS size_sq_ft,
    split_part(weekdays_hours,'-',1)::time as weekdays_opening_hour,
    split_part(weekdays_hours,'-',2)::time as weekdays_closing_hour,
    split_part(weekends_hours,'-',1)::time as weekends_opening_hour,
    split_part(weekends_hours,'-',2)::time as weekends_closing_hour,
    split_part(holidays_hours,'-',1)::time as holidays_opening_hour,
    split_part(holidays_hours,'-',2)::time as holidays_closing_hour,
    CASE WHEN zip_code IS NULL THEN '' ELSE zip_code END AS zip_code,
    CASE WHEN is_active IS NULL THEN FALSE ELSE is_active END AS is_active,
    case
        WHEN size_sq_ft < 5000 THEN 'Small' 
        WHEN size_sq_ft BETWEEN 5000 AND 10000 THEN 'Medium' 
        WHEN size_sq_ft > 10000 THEN 'Large'
    end as store_size_category,
    datediff(year,opening_date,current_date()) as store_age_years,
    round((current_sales / sales_target) * 100,2) as sales_target_achievement_percentage,
    round(current_sales / size_sq_ft,2) as revenue_per_sq_ft,
    round(current_sales / employee_count,2) as employee_efficiency,
    case when round((current_sales / sales_target) * 100,2)<90 then 1 else 0 end as performance_issues
FROM silver_layer.store_latest
