{% macro string_validation(string_column) %}
case 
    when {{string_column}} is null then 
        'NA'
        else 
            TRIM(CONCAT(
                UPPER(SUBSTRING({{string_column}}, 1, 1)),
                LOWER(SUBSTRING({{string_column}}, 2, LENGTH({{string_column}})))
            ))
    end 
{% endmacro %}