{% macro edr_date_trunc(date_part, date_expression) -%}
    {{ return(adapter.dispatch('edr_date_trunc', 'elementary') (date_part, date_expression)) }}
{%- endmacro %}

{% macro default__edr_date_trunc(datepart, date_expression) %}
    {% set macro = dbt.date_trunc or dbt_utils.date_trunc %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `date_trunc` macro.") }}
    {% endif %}
    {{ return(macro(datepart, date_expression)) }}
{% endmacro %}

{# Bigquery date_trunc does not support timestamp expressions and date parts smaller than day #}
{% macro bigquery__edr_date_trunc(date_part, date_expression) %}
    timestamp_trunc(cast({{ date_expression }} as timestamp), {{ date_part }})
{% endmacro %}
