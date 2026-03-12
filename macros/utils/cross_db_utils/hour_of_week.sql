{% macro edr_hour_of_week_expression(date_expr) %}
    {{
        return(
            adapter.dispatch("edr_hour_of_week_expression", "elementary")(
                elementary.edr_cast_as_timestamp(date_expr)
            )
        )
    }}
{% endmacro %}

{# Databricks, Spark: #}
{% macro default__edr_hour_of_week_expression(date_expr) %}
    concat(
        cast(
            date_format({{ date_expr }}, 'EEEE') as {{ elementary.edr_type_string() }}
        ),
        cast(hour({{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}

{% macro bigquery__edr_hour_of_week_expression(date_expr) %}
    concat(
        cast(format_date('%A', {{ date_expr }}) as {{ elementary.edr_type_string() }}),
        cast(extract(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}

{% macro postgres__edr_hour_of_week_expression(date_expr) %}
    concat(
        cast(to_char({{ date_expr }}, 'Day') as {{ elementary.edr_type_string() }}),
        cast(extract(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}

{% macro redshift__edr_hour_of_week_expression(date_expr) %}
    concat(
        trim(' ' from to_char({{ date_expr }}, 'Day')),
        cast(extract(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}

{% macro snowflake__edr_hour_of_week_expression(date_expr) %}
    concat(
        cast(
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
            ) as {{ elementary.edr_type_string() }}
        ),
        cast(hour({{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}

{% macro athena__edr_hour_of_week_expression(date_expr) %}
    date_format({{ date_expr }}, '%W%H')
{% endmacro %}

{% macro trino__edr_hour_of_week_expression(date_expr) %}
    date_format({{ date_expr }}, '%W%H')
{% endmacro %}

-- fmt: off
{% macro clickhouse__edr_hour_of_week_expression(date_expr) %}
    concat(formatDateTime({{ date_expr }}, '%W'), formatDateTime({{ date_expr }}, '%H'))
{% endmacro %}
-- fmt: on

{% macro vertica__edr_hour_of_week_expression(date_expr) %}
    concat(
        cast(
            trim(
                ' ' from to_char({{ date_expr }}, 'Day')
            ) as {{ elementary.edr_type_string() }}
        ),
        cast(extract(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}

{% macro fabric__edr_hour_of_week_expression(date_expr) %}
    concat(
        cast(datename(weekday, {{ date_expr }}) as {{ elementary.edr_type_string() }}),
        cast(datepart(hour, {{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}

{% macro duckdb__edr_hour_of_week_expression(date_expr) %}
    concat(
        cast(dayname({{ date_expr }}) as {{ elementary.edr_type_string() }}),
        cast(extract(hour from {{ date_expr }}) as {{ elementary.edr_type_string() }})
    )
{% endmacro %}
