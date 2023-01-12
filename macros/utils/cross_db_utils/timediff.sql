{# Same as datediff, but supports timestamps as well and not just dates #}
{% macro timediff(timepart, first_timestamp, second_timestamp) -%}
    {{ return(adapter.dispatch('timediff', 'elementary')(timepart, first_timestamp, second_timestamp)) }}
{%- endmacro %}

{# Snowflake #}
{% macro default__timediff(timepart, first_timestamp, second_timestamp) %}
    datediff({{ timepart }}, {{ first_timestamp }}, {{ second_timestamp }})
{% endmacro %}

{% macro bigquery__timediff(timepart, first_timestamp, second_timestamp) %}
    timestamp_diff({{ second_timestamp }}, {{ first_timestamp }}, {{ timepart }})
{% endmacro %}

{% macro redshift__timediff(timepart, first_timestamp, second_timestamp) %}
    datediff({{ timepart }}, {{ first_timestamp }}, {{ second_timestamp }})
{% endmacro %}

{% macro postgres__timediff(timepart, first_timestamp, second_timestamp) %}
    extract(epoch from {{ second_timestamp }} - {{ first_timestamp }}) / extract(epoch from interval '1 {{ timepart }}')
{% endmacro %}
