{% macro get_compare_columns(compare_columns) %}
    {% set compare_cols_csv = compare_columns | join(', ') %}
{% endmacro %}