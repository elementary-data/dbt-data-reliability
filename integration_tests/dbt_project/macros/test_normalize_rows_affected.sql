{% macro test_normalize_rows_affected(rows_affected) %}
    {% set result = elementary.normalize_rows_affected(rows_affected) %}
    {{ return(result) }}
{% endmacro %}
