{# Same as datediff, but supports timestamps as well and not just dates #}
{% macro timediff(timepart, first_timestamp, second_timestamp) -%}
    {{ return(adapter.dispatch('timediff', 'elementary')(timepart, first_timestamp, second_timestamp)) }}
{%- endmacro %}

{# For Snowflake, Databricks, Redshift, Postgres & Spark #}
{# the dbt adapter implementation supports both timestamp and dates #}
{% macro default__timediff(timepart, first_timestamp, second_timestamp) %}
    {{ elementary.edr_datediff(first_timestamp, second_timestamp, timepart)}}
{% endmacro %}

{% macro bigquery__timediff(timepart, first_timestamp, second_timestamp) %}
    timestamp_diff({{ second_timestamp }}, {{ first_timestamp }}, {{ timepart }})
{% endmacro %}
