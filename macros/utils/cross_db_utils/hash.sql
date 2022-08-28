{% macro hash(field) %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.hash(field)) }}
    {% else %}
        {{ return(dbt_utils.hash(field)) }}
    {% endif %}
{% endmacro %}
