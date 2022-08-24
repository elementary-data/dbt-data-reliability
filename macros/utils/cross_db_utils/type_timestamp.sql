{% macro type_timestamp() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_timestamp()) }}
    {% else %}
        {{ return(dbt_utils.type_timestamp()) }}
    {% endif %}
{% endmacro %}
