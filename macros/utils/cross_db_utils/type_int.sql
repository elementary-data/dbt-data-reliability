{% macro type_int() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_int()) }}
    {% else %}
        {{ return(dbt_utils.type_int()) }}
    {% endif %}
{% endmacro %}
