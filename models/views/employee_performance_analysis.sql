select sum(fs.total_sales_amount) as sum_of_total_sales_amount,e.employee_id from {{ ref('fact_sales') }} fs join {{ ref('dim_employee') }} e on e.employee_key=fs.employee_key group by e.employee_id order by sum(fs.total_sales_amount) desc


