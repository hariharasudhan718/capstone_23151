{% macro phone_number_validation(phone_number_column) %} 
regexp_replace(
    regexp_replace(
        regexp_replace({{phone_number_column}}, '^\+1', ''), -- Remove leading '+1'
        '[\+\(\)\.\s]', ''), -- Remove special characters and spaces
    '[^0-9X]', '')
{% endmacro %}