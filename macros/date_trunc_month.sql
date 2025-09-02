{% macro date_trunc_month(column_name) %}
    DATE_TRUNC('MONTH', {{ column_name }})
{% endmacro %}
