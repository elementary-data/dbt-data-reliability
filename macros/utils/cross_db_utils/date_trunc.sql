{% macro date_trunc(datepart, date) -%}
    {{ return(adapter.dispatch('date_trunc', 'elementary') (datepart, date)) }}
{%- endmacro %}

{% macro default__date_trunc(datepart, date) %}
    {% set macro = dbt.date_trunc or dbt_utils.date_trunc %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `date_trunc` macro.") }}
    {% endif %}
    {{ return(macro(datepart, date)) }}
{% endmacro %}

{% macro sqlserver__date_trunc(datepart, date) %}
    datepart({{datepart}}, {{date}}) 
{% endmacro %}
