{% macro cents_to_dollars(column_name, scale=2) %}
    ROUND({{ column_name }} / 100.0, {{ scale }})
{% endmacro %}