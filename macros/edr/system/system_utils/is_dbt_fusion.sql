{% macro is_dbt_fusion() %}
    {% if dbt_version.split(".")[0] | int == 2 %}
        {% do return(true) %}
    {% endif %}

    {% do return(false) %}
{% endmacro %}