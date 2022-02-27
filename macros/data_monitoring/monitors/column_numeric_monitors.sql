{% macro max_monitor(column_name) -%}
    max({{ column_name }})
{%- endmacro %}

{% macro min_monitor(column_name) -%}
    min({{ column_name }})
{%- endmacro %}