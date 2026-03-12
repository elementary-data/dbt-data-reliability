{% macro edr_day_of_week_expression(date_expr) %}
    {{
        return(
            adapter.dispatch("edr_day_of_week_expression", "elementary")(
                elementary.edr_cast_as_date(date_expr)
            )
        )
    }}
{% endmacro %}

{# Databricks, Spark: #}
{% macro default__edr_day_of_week_expression(date_expr) %}
    date_format({{ date_expr }}, 'EEEE')
{% endmacro %}

{% macro bigquery__edr_day_of_week_expression(date_expr) %}
    format_date('%A', {{ date_expr }})
{% endmacro %}

{% macro postgres__edr_day_of_week_expression(date_expr) %}
    to_char({{ date_expr }}, 'Day')
{% endmacro %}

{% macro redshift__edr_day_of_week_expression(date_expr) %}
    {# Redshift returns the days padded with whitespaces to width of 9 #}
    trim(' ' from to_char({{ date_expr }}, 'Day'))
{% endmacro %}

{% macro snowflake__edr_day_of_week_expression(date_expr) %}
    {# copied from Snowflake help docs: https://docs.snowflake.com/en/user-guide/date-time-examples #}
    decode(
        extract('dayofweek',{{ date_expr }}),
        1,
        'Monday',
        2,
        'Tuesday',
        3,
        'Wednesday',
        4,
        'Thursday',
        5,
        'Friday',
        6,
        'Saturday',
        0,
        'Sunday'
    )
{% endmacro %}

{% macro athena__edr_day_of_week_expression(date_expr) %}
    date_format({{ date_expr }}, '%W')
{% endmacro %}

{% macro trino__edr_day_of_week_expression(date_expr) %}
    date_format({{ date_expr }}, '%W')
{% endmacro %}

{% macro duckdb__edr_day_of_week_expression(date_expr) %}
    dayname({{ date_expr }})
{% endmacro %}

{% macro dremio__edr_day_of_week_expression(date_expr) %}
    to_char({{ date_expr }}, 'DAY')
{% endmacro %}

{% macro vertica__edr_day_of_week_expression(date_expr) %}
    trim(' ' from to_char({{ date_expr }}, 'Day'))
{% endmacro %}

{% macro fabric__edr_day_of_week_expression(date_expr) %}
    cast(datename(weekday, {{ date_expr }}) as varchar(30))
{% endmacro %}

-- fmt: off
{% macro clickhouse__edr_day_of_week_expression(date_expr) %}
    formatDateTime({{ date_expr }}, '%W')
{% endmacro %}
-- fmt: on
