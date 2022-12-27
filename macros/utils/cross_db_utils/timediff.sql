{# Same as datediff, but supports timestamps as well and not just dates #}
{% macro timediff(timepart, first_timestamp, second_timestamp) -%}
    {{ return(adapter.dispatch('timediff', 'elementary')(timepart, first_timestamp, second_timestamp)) }}
{%- endmacro %}

{# Snowflake and Redshift #}
{% macro default__timediff(timepart, first_timestamp, second_timestamp) %}
    datediff({{ timepart }}, {{ first_timestamp }}, {{ second_timestamp }})
{% endmacro %}

{% macro bigquery__timediff(timepart, first_timestamp, second_timestamp) %}
    timestamp_diff({{ first_timestamp }}, {{ second_timestamp }}, {{ timepart }})
{% endmacro %}
