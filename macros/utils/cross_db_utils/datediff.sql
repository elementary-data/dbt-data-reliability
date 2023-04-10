{% macro edr_datediff(first_date, second_date, date_part) %}
    {{ return(adapter.dispatch('edr_datediff', 'elementary')(first_date, second_date, date_part)) }}
{% endmacro %}

{# For Snowflake, Databricks, Redshift, Postgres & Spark #}
{# the dbt adapter implementation supports both timestamp and dates #}
{% macro default__edr_datediff(first_date, second_date, date_part) %}
    {% set macro = dbt.datediff or dbt_utils.datediff %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `datediff` macro.") }}
    {% endif %}
    {{ return(macro(first_date, second_date, date_part)) }}
{% endmacro %}

{% macro bigquery__edr_datediff(first_date, second_date, date_part) %}
    {%- if date_part | lower in ['second', 'minute', 'hour', 'day'] %}
        timestamp_diff({{ second_date }}, {{ first_date }}, {{ date_part }})
    {%- elif date_part | lower in ['week', 'month', 'quarter', 'year'] %}
        {% set macro = dbt.datediff or dbt_utils.datediff %}
        {% if not macro %}
            {{ exceptions.raise_compiler_error("Did not find a `datediff` macro.") }}
        {% endif %}
        {{ return(macro(elementary.edr_cast_as_date(first_date), elementary.edr_cast_as_date(second_date), date_part)) }}
    {%- else %}
        {{ exceptions.raise_compiler_error("Unsupported date_part in edr_datediff: ".format(date_part)) }}
    {%- endif %}
{% endmacro %}