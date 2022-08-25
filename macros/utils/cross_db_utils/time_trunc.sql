{# Same as date trunc, but supports timestamps as well and not just dates #}
{% macro time_trunc(date_part, date_expression) -%}
    {{ return(adapter.dispatch('time_trunc', 'elementary') (date_part, date_expression)) }}
{%- endmacro %}

{% macro default__time_trunc(date_part, date_expression) %}
    date_trunc('{{date_part}}', cast({{ date_expression }} as {{ elementary.type_timestamp() }}))
{% endmacro %}

{% macro bigquery__time_trunc(date_part, date_expression) %}
    timestamp_trunc(cast({{ date_expression }} as timestamp), {{ date_part }})
{% endmacro %}