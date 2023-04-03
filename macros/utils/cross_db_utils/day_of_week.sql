{% macro edr_day_of_week_expression(date_expr) %}
    {{ return(adapter.dispatch('edr_day_of_week_expression','elementary')(elementary.edr_cast_as_date(date_expr))) }}
{% endmacro %}

{# Snowflake, Spark: dayofweek function #}
{% macro default__edr_day_of_week_expression(date_expr) %}
    dayofweek({{ date_expr }})
{% endmacro %}

{% macro bigquery__edr_day_of_week_expression(date_expr) %}
    extract(dayofweek from {{ date_expr }})
{% endmacro %}

{% macro redshift__edr_day_of_week_expression(date_expr) %}
    date_part(dayofweek, {{ date_expr }})
{% endmacro %}

{% macro postgres__edr_day_of_week_expression(date_expr) %}
    extract(dow from {{ date_expr }})
{% endmacro %}