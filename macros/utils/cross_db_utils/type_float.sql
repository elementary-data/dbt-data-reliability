{% macro type_float() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_float()) }}
    {% else %}
        {{ return(dbt_utils.type_float()) }}
    {% endif %}
{% endmacro %}
