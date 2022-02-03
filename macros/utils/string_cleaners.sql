{% macro trim_quotes(column_to_trim) %}
    trim({{ column_to_trim }},'"')
{% endmacro %}

{% macro low_no_quotes(column_to_trim) %}
    lower(replace({{ column_to_trim }}, '"', ''))
{% endmacro %}
