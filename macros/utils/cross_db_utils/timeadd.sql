{# Same as dateadd, but supports timestamps as well and not just dates #}
{% macro edr_timeadd(date_part, number, timestamp_expression) -%}
    {{ return(adapter.dispatch('edr_timeadd', 'elementary')(date_part, number, timestamp_expression)) }}
{%- endmacro %}

{# Snowflake #}
{% macro default__edr_timeadd(date_part, number, timestamp_expression) %}
    dateadd({{ date_part }}, {{ number }}, {{ elementary.edr_cast_as_timestamp(timestamp_expression) }})
{% endmacro %}

{% macro bigquery__edr_timeadd(date_part, number, timestamp_expression) %}
    timestamp_add({{ elementary.edr_cast_as_timestamp(timestamp_expression) }}, INTERVAL {{ number }} {{ date_part }})
{% endmacro %}

{% macro postgres__edr_timeadd(date_part, number, timestamp_expression) %}
    {{ elementary.edr_cast_as_timestamp(timestamp_expression) }} + {{ number }} * INTERVAL '1 {{ date_part }}'
{% endmacro %}

{% macro redshift__edr_timeadd(date_part, number, timestamp_expression) %}
    dateadd({{ date_part }}, {{ number }}, {{ elementary.edr_cast_as_timestamp(timestamp_expression) }})
{% endmacro %}