{% macro not(condition) %}
    {{ adapter.dispatch('not','elementary')(condition) }}
{% endmacro %}

{% macro default__not(condition) -%}
    {{ not condition }}
{%- endmacro %}

{% macro sqlserver__not(condition) -%}
    {{ cast_as_bool(quote(condition)) }} = 0
{%- endmacro %}
