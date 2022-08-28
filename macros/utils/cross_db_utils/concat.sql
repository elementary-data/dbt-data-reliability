{% macro concat(fields) %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.concat(fields)) }}
    {% else %}
        {{ return(dbt_utils.concat(fields)) }}
    {% endif %}
{% endmacro %}
