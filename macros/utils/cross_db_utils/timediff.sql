{# Same as datediff, but supports timestamps as well and not just dates #}
{% macro edr_timediff(timepart, first_timestamp, second_timestamp) -%}
    {{ return(adapter.dispatch('edr_timediff', 'elementary')(timepart, first_timestamp, second_timestamp)) }}
{%- endmacro %}

{# Snowflake #}
{% macro default__edr_timediff(timepart, first_timestamp, second_timestamp) %}
    datediff({{ timepart }}, {{ first_timestamp }}, {{ second_timestamp }})
{% endmacro %}

{% macro bigquery__edr_timediff(timepart, first_timestamp, second_timestamp) %}
    timestamp_diff({{ second_timestamp }}, {{ first_timestamp }}, {{ timepart }})
{% endmacro %}

{% macro redshift__edr_timediff(timepart, first_timestamp, second_timestamp) %}
    datediff({{ timepart }}, {{ first_timestamp }}, {{ second_timestamp }})
{% endmacro %}

{% macro postgres__edr_timediff(timepart, first_timestamp, second_timestamp) %}
    extract(epoch from {{ second_timestamp }} - {{ first_timestamp }}) / extract(epoch from interval '1 {{ timepart }}')
{% endmacro %}
