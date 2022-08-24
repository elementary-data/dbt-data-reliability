{% macro datediff(first_date, second_date, datepart) %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.datediff(first_date, second_date, datepart)) }}
    {% else %}
        {{ return(dbt_utils.datediff(first_date, second_date, datepart)) }}
    {% endif %}
{% endmacro %}
