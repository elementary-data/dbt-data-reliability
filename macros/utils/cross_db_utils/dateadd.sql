{% macro dateadd(datepart, interval, from_date_or_timestamp) %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.dateadd(datepart, interval, from_date_or_timestamp)) }}
    {% else %}
        {{ return(dbt_utils.dateadd(datepart, interval, from_date_or_timestamp)) }}
    {% endif %}
{% endmacro %}
