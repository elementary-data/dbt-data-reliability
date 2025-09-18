{% macro edr_datediff(first_date, second_date, date_part) %}
    {{ return(adapter.dispatch('edr_datediff', 'elementary')(first_date, second_date, date_part)) }}
{% endmacro %}

{# For Snowflake, Databricks, Redshift, Postgres #}
{# the dbt adapter implementation supports both timestamp and dates #}
{% macro default__edr_datediff(first_date, second_date, date_part) %}
    {% set macro = dbt.datediff or dbt_utils.datediff %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `datediff` macro.") }}
    {% endif %}
    {{ return(macro(first_date, second_date, date_part)) }}
{% endmacro %}

{% macro clickhouse__edr_datediff(first_date, second_date, date_part) %}
    {%- set first_date_expr = elementary.edr_cast_as_timestamp(first_date) if first_date is string else first_date -%}
    {%- set second_date_expr = elementary.edr_cast_as_timestamp(second_date) if second_date is string else second_date -%}
    coalesce(dateDiff('{{ date_part }}', {{ first_date_expr }}, {{ second_date_expr }}), 0)::Nullable(Int32)
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

{# dbt-spark implementation has an off by one for datepart == "hour" #}
{# because it uses CEIL instead of FLOOR #}
{% macro spark__edr_datediff(first_date, second_date, datepart) %}
    {%- if datepart in ['day', 'week', 'month', 'quarter', 'year'] -%}

        {# make sure the dates are real, otherwise raise an error asap #}
        {% set first_date = assert_not_null('date', first_date) %}
        {% set second_date = assert_not_null('date', second_date) %}

    {%- endif -%}

    {%- if datepart == 'day' -%}

        datediff({{second_date}}, {{first_date}})

    {%- elif datepart == 'week' -%}

        case when {{first_date}} < {{second_date}}
            then floor(datediff({{second_date}}, {{first_date}})/7)
            else ceil(datediff({{second_date}}, {{first_date}})/7)
            end

        -- did we cross a week boundary (Sunday)?
        + case
            when {{first_date}} < {{second_date}} and dayofweek({{second_date}}) < dayofweek({{first_date}}) then 1
            when {{first_date}} > {{second_date}} and dayofweek({{second_date}}) > dayofweek({{first_date}}) then -1
            else 0 end

    {%- elif datepart == 'month' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(date({{second_date}}), date({{first_date}})))
            else ceil(months_between(date({{second_date}}), date({{first_date}})))
            end

        -- did we cross a month boundary?
        + case
            when {{first_date}} < {{second_date}} and dayofmonth({{second_date}}) < dayofmonth({{first_date}}) then 1
            when {{first_date}} > {{second_date}} and dayofmonth({{second_date}}) > dayofmonth({{first_date}}) then -1
            else 0 end

    {%- elif datepart == 'quarter' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(date({{second_date}}), date({{first_date}}))/3)
            else ceil(months_between(date({{second_date}}), date({{first_date}}))/3)
            end

        -- did we cross a quarter boundary?
        + case
            when {{first_date}} < {{second_date}} and (
                (dayofyear({{second_date}}) - (quarter({{second_date}}) * 365/4))
                < (dayofyear({{first_date}}) - (quarter({{first_date}}) * 365/4))
            ) then 1
            when {{first_date}} > {{second_date}} and (
                (dayofyear({{second_date}}) - (quarter({{second_date}}) * 365/4))
                > (dayofyear({{first_date}}) - (quarter({{first_date}}) * 365/4))
            ) then -1
            else 0 end

    {%- elif datepart == 'year' -%}

        year({{second_date}}) - year({{first_date}})

    {%- elif datepart in ('hour', 'minute', 'second', 'millisecond', 'microsecond') -%}

        {%- set divisor -%}
            {%- if datepart == 'hour' -%} 3600
            {%- elif datepart == 'minute' -%} 60
            {%- elif datepart == 'second' -%} 1
            {%- elif datepart == 'millisecond' -%} (1/1000)
            {%- elif datepart == 'microsecond' -%} (1/1000000)
            {%- endif -%}
        {%- endset -%}

        case when {{first_date}} < {{second_date}}
            then floor((
                {# make sure the timestamps are real, otherwise raise an error asap #}
                {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', second_date)) }}
                - {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', first_date)) }}
            ) / {{divisor}})
            else ceil((
                {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', second_date)) }}
                - {{ assert_not_null('to_unix_timestamp', assert_not_null('to_timestamp', first_date)) }}
            ) / {{divisor}})
            end

            {% if datepart == 'millisecond' %}
                + cast(date_format({{second_date}}, 'SSS') as int)
                - cast(date_format({{first_date}}, 'SSS') as int)
            {% endif %}

            {% if datepart == 'microsecond' %}
                {% set capture_str = '[0-9]{4}-[0-9]{2}-[0-9]{2}.[0-9]{2}:[0-9]{2}:[0-9]{2}.([0-9]{6})' %}
                -- Spark doesn't really support microseconds, so this is a massive hack!
                -- It will only work if the timestamp-string is of the format
                -- 'yyyy-MM-dd-HH mm.ss.SSSSSS'
                + cast(regexp_extract({{second_date}}, '{{capture_str}}', 1) as int)
                - cast(regexp_extract({{first_date}}, '{{capture_str}}', 1) as int)
            {% endif %}

    {%- else -%}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Spark") }}

    {%- endif -%}

{% endmacro %}

{% macro athena__edr_datediff(first_date, second_date, date_part) %}
    {% set macro = dbt.datediff or dbt_utils.datediff %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `datediff` macro.") }}
    {% endif %}
    {{ return(macro(elementary.edr_cast_as_timestamp(first_date), elementary.edr_cast_as_timestamp(second_date), date_part)) }}
{% endmacro %}

{% macro trino__edr_datediff(first_date, second_date, date_part) %}
    {% set macro = dbt.datediff or dbt_utils.datediff %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `datediff` macro.") }}
    {% endif %}
    {{ return(macro(elementary.edr_cast_as_timestamp(first_date), elementary.edr_cast_as_timestamp(second_date), date_part)) }}
{% endmacro %}

{% macro dremio__edr_datediff(first_date, second_date, date_part) %}
    {%- set seconds_diff_expr -%}
        cast(unix_timestamp(substr(cast(({{ second_date }}) as varchar), 1, 19)) - 
            unix_timestamp(substr(cast(({{ first_date }}) as varchar), 1, 19)) as integer)
    {%- endset -%}
    
    {%- set first_date_ts = elementary.edr_cast_as_timestamp(first_date) -%}
    {%- set second_date_ts = elementary.edr_cast_as_timestamp(second_date) -%}

    {# This macro is copied from dbt-dremio, but we replaced entirely the usage of TIMESTAMPDIFF
       as for some reason it must be used with "select" - which creates issues. 
       So we're using an alternative implementation in these cases using the seconds diff expression above.

       See original implementation here - https://github.com/dremio/dbt-dremio/blob/22588446edabae1670d929e27501ae3060fdd0bc/dbt/include/dremio/macros/utils/date_spine.sql#L53
    #}

    {% if date_part == 'year' %}
        (EXTRACT(YEAR FROM {{second_date_ts}}) - EXTRACT(YEAR FROM {{first_date_ts}})) 
    {% elif date_part == 'quarter' %}
        ((EXTRACT(YEAR FROM {{second_date_ts}}) - EXTRACT(YEAR FROM {{first_date_ts}})) * 4 + CEIL(EXTRACT(MONTH FROM {{second_date_ts}}) / 3.0) - CEIL(EXTRACT(MONTH FROM {{first_date_ts}}) / 3.0))
    {% elif date_part == 'month' %}
        ((EXTRACT(YEAR FROM {{second_date_ts}}) - EXTRACT(YEAR FROM {{first_date_ts}})) * 12 + (EXTRACT(MONTH FROM {{second_date_ts}}) - EXTRACT(MONTH FROM {{first_date_ts}})))
    {% elif date_part == 'weekday' %}
        CAST(CAST({{second_date_ts}} AS DATE) - CAST({{first_date_ts}} AS DATE) AS INTEGER)
    {% elif date_part == 'week' %}
        ({{ seconds_diff_expr }} / (60 * 60 * 24 * 7))
    {% elif date_part == 'day' %}
        ({{ seconds_diff_expr }} / (60 * 60 * 24))
    {% elif date_part == 'hour' %}
        ({{ seconds_diff_expr }} / (60 * 60))
    {% elif date_part == 'minute' %}
        ({{ seconds_diff_expr }} / 60)
    {% elif date_part == 'second' %}
        {{ seconds_diff_expr }}
    {% else %}
        {% do exceptions.raise_compiler_error('Unsupported date part: ' ~ date_part) %}
    {% endif %}
{% endmacro %}
