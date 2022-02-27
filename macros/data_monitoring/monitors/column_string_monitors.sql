{% macro max_length_monitor(column_name) -%}
    max(length({{ column_name }}))
{%- endmacro %}

{% macro min_length_monitor(column_name) -%}
    min(length({{ column_name }}))
{%- endmacro %}