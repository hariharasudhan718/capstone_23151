{% macro email_validation(email_column) %} 
case
    when REGEXP_LIKE({{email_column}},'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') then TRIM({{email_column}})
    else 'NA'
end 
{% endmacro %}
