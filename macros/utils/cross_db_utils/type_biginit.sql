{% macro type_biginit() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_biginit()) }}
    {% else %}
        {{ return(dbt_utils.type_biginit()) }}
    {% endif %}
{% endmacro %}
