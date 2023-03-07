{% macro and() %}
    {{ adapter.dispatch('and','elementary')() }}
{% endmacro %}

{% macro default__and() -%}
    ||
{%- endmacro %}

{% macro sqlserver__and() -%}
    +
{%- endmacro %}