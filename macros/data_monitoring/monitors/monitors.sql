
{% macro row_count_monitor() -%}
    count(1)
{%- endmacro %}

{% macro max_monitor(col) -%}
    max( {{ col }} )
{%- endmacro %}

{% macro min_monitor(col) -%}
    min( {{ col }} )
{%- endmacro %}