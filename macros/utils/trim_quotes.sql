{% macro trim_quotes(column_to_trim) %}
    trim({{ column_to_trim }},'"')
{% endmacro %}
