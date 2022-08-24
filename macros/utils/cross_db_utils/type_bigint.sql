{% macro type_bigint() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_bigint()) }}
    {% else %}
        {{ return(dbt_utils.type_bigint()) }}
    {% endif %}
{% endmacro %}
