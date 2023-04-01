{% macro edr_day_of_week_expression(date_expr) %}
    {% set date_expr_casted_as_date = elementary.edr_cast_as_date(date_expr) %}
    {% set result = adapter.dispatch('edr_day_of_week_expression','elementary')(date_expr_casted_as_date) %}
    {{ return(result) }}
{% endmacro %}

{# Snowflake, Spark: dayofweek function #}
{% macro default__edr_day_of_week_expression(date_expr) %}
    {% set dayofweek_expr %}
    dayofweek({{ date_expr }})
    {% endset %}
   {{return (dayofweek_expr) }}
{% endmacro %}

{% macro bigquery__edr_day_of_week_expression(date_expr) %}
    {% set dayofweek_expr %}
    extract(dayofweek from {{ date_expr }})
    {% endset %}
   {{return (dayofweek_expr) }}
{% endmacro %}

{% macro redshift__edr_day_of_week_expression(date_expr) %}
    {% set dayofweek_expr %}
    date_part(dayofweek, {{ date_expr }})
    {% endset %}
   {{return (dayofweek_expr) }}
{% endmacro %}

{% macro postgres__edr_day_of_week_expression(date_expr) %}
    {% set dayofweek_expr %}
    extract(dow from {{ date_expr }})
    {% endset %}
   {{return (dayofweek_expr) }}
{% endmacro %}