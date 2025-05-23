{% macro date_validation(date_column) %}
    case when
    coalesce(
        try_to_date({{ date_column }}, 'YYYY/MM/DD'),
        try_to_date({{ date_column }}, 'YYYY-MM-DD'),
        try_to_date({{ date_column }}, 'DD/MM/YYYY'),
        try_to_date({{ date_column }}, 'DD-MM-YYYY'),
        try_to_date({{ date_column }}, 'MM-DD-YYYY'),
        try_to_date({{ date_column }}, 'MM/DD/YYYY'),
        try_to_date({{ date_column }}, 'YYYY/DD/MM'),
        try_to_date({{ date_column }}, 'YYYY-DD-MM')
    )  is null then try_to_date('9999/12/31','YYYY/MM/DD')
    else 
    coalesce(
        try_to_date({{ date_column }}, 'YYYY/MM/DD'),
        try_to_date({{ date_column }}, 'YYYY-MM-DD'),
        try_to_date({{ date_column }}, 'DD/MM/YYYY'),
        try_to_date({{ date_column }}, 'DD-MM-YYYY'),
        try_to_date({{ date_column }}, 'MM-DD-YYYY'),
        try_to_date({{ date_column }}, 'MM/DD/YYYY'),
        try_to_date({{ date_column }}, 'YYYY/DD/MM'),
        try_to_date({{ date_column }}, 'YYYY-DD-MM')
    )
    end
{% endmacro %}