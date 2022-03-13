{% macro date_trunc(date_part, date_expression) -%}
    {{ return(adapter.dispatch('date_trunc', 'elementary') (date_part, date_expression)) }}
{%- endmacro %}

{% macro default__date_trunc(date_part, date_expression) %}
    date_trunc('{{date_part}}', cast({{ date_expression }} as {{ dbt_utils.type_timestamp() }}))
{% endmacro %}

{% macro bigquery__date_trunc(date_part, date_expression) %}
    date_trunc(cast({{ date_expression }} as timestamp), {{ date_part }})
{% endmacro %}