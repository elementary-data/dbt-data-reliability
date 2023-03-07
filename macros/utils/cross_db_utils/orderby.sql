{% macro orderby(condition) %}
    {{ adapter.dispatch('orderby','elementary')(condition) }}
{% endmacro %}

{% macro default__orderby(condition) -%}
    ORDER BY
{%- endmacro %}

{% macro sqlserver__orderby(condition) -%}
    ORDER BY {{ condition }} OFFSET 0 ROWS
{%- endmacro %}