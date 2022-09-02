{% macro safe_cast(field, type) %}
    {% if target.type == 'databricks' %}
        try_cast({{field}} as {{type}})
    {% elif dbt_version >= '1.2.0' %}
        {{ return(dbt.safe_cast(field, type)) }}
    {% else %}
        {{ return(dbt_utils.safe_cast(field, type)) }}
    {% endif %}
{% endmacro %}
