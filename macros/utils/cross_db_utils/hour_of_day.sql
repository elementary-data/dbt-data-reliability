{% macro edr_hour_of_day_expression(date_expr) %}
    {{ return(adapter.dispatch('edr_hour_of_day_expression','elementary')(elementary.edr_cast_as_timestamp(date_expr))) }}
{% endmacro %}

{# Databricks, Spark, Athena, Trino: #}
{% macro default__edr_hour_of_day_expression(date_expr) %}
    HOUR({{ date_expr }})
{% endmacro %}

{% macro bigquery__edr_hour_of_day_expression(date_expr) %}
    EXTRACT(hour from {{ date_expr }})
{% endmacro %}

{% macro postgres__edr_hour_of_day_expression(date_expr) %}
    EXTRACT(hour from {{ date_expr }})
{% endmacro %}

{% macro redshift__edr_hour_of_day_expression(date_expr) %}
    EXTRACT(hour from {{ date_expr }})
{% endmacro %}

{% macro snowflake__edr_hour_of_day_expression(date_expr) %}
    HOUR({{ date_expr }})
{% endmacro %}
