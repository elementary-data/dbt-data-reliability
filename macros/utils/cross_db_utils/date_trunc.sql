{% macro date_trunc(datepart, date) %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.date_trunc(datepart, date)) }}
    {% else %}
        {{ return(dbt_utils.date_trunc(datepart, date)) }}
    {% endif %}
{% endmacro %}
