{% macro edr_hour_of_week_expression(date_expr) %}
    {{ return(adapter.dispatch('edr_hour_of_week_expression','elementary')(elementary.edr_cast_as_timestamp(date_expr))) }}
{% endmacro %}

{# Databricks, Spark: #}
{% macro default__edr_hour_of_week_expression(date_expr) %}
    concat(cast(DATE_FORMAT({{ date_expr }}, 'EEEE') as {{ elementary.edr_type_string() }}), cast(HOUR({{ date_expr }}) as {{ elementary.edr_type_string() }}))
{% endmacro %}

{% macro bigquery__edr_hour_of_week_expression(date_expr) %}
    concat(cast(FORMAT_DATE('%A', {{ date_expr }}) as {{ elementary.edr_type_string() }}), cast(EXTRACT(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }}))
{% endmacro %}

{% macro postgres__edr_hour_of_week_expression(date_expr) %}
    concat(cast(to_char({{ date_expr }}, 'Day') as {{ elementary.edr_type_string() }}), cast(EXTRACT(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }}))
{% endmacro %}

{% macro redshift__edr_hour_of_week_expression(date_expr) %}
    concat(trim(' ' FROM to_char({{ date_expr }}, 'Day')), cast(EXTRACT(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }}))
{% endmacro %}

{% macro snowflake__edr_hour_of_week_expression(date_expr) %}
    concat(cast(DECODE (EXTRACT('dayofweek',{{ date_expr }}),
    1 , 'Monday',
    2 , 'Tuesday',
    3 , 'Wednesday',
    4 , 'Thursday',
    5 , 'Friday',
    6 , 'Saturday',
    0 , 'Sunday'
    ) as {{ elementary.edr_type_string() }}),  cast(HOUR({{ date_expr }}) as {{ elementary.edr_type_string() }}))
{% endmacro %}

{% macro athena__edr_hour_of_week_expression(date_expr) %}
    DATE_FORMAT({{ date_expr }}, '%W%H')
{% endmacro %}

{% macro trino__edr_hour_of_week_expression(date_expr) %}
    date_format({{ date_expr }}, '%W%H')
{% endmacro %}
