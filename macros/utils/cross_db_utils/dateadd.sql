{% macro edr_dateadd(datepart, interval, from_date_or_timestamp) %}
    {{
        return(
            adapter.dispatch("edr_dateadd", "elementary")(
                datepart, interval, from_date_or_timestamp
            )
        )
    }}
{% endmacro %}

{% macro default__edr_dateadd(datepart, interval, from_date_or_timestamp) %}
    {% set macro = dbt.dateadd or dbt_utils.dateadd %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `dateadd` macro.") }}
    {% endif %}
    {{ return(macro(datepart, interval, from_date_or_timestamp)) }}
{% endmacro %}

{#
  Override dbt-dremio's dateadd macro which has two bugs:
  1. Calls interval.replace() on the interval parameter, failing when interval is an integer
  2. Wraps result in "select TIMESTAMPADD(...)" which creates a scalar subquery when
     embedded in larger SQL expressions, causing $SCALAR_QUERY errors in Dremio

  This override outputs just TIMESTAMPADD(...) as an expression (no "select" prefix).
#}
{% macro fabric__edr_dateadd(datepart, interval, from_date_or_timestamp) %}
    dateadd({{ datepart }}, {{ interval }}, {{ from_date_or_timestamp }})
{% endmacro %}

{% macro dremio__edr_dateadd(datepart, interval, from_date_or_timestamp) %}
    {% set datepart = datepart | lower %}
    {% if datepart == "year" %}
        timestampadd(
            year,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% elif datepart == "quarter" %}
        timestampadd(
            quarter,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% elif datepart == "month" %}
        timestampadd(
            month,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% elif datepart == "week" %}
        timestampadd(
            week,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% elif datepart == "hour" %}
        timestampadd(
            hour,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% elif datepart == "minute" %}
        timestampadd(
            minute,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% elif datepart == "second" %}
        timestampadd(
            second,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% elif datepart == "day" %}
        timestampadd(
            day,
            cast({{ interval }} as int),
            cast({{ from_date_or_timestamp }} as timestamp)
        )
    {% else %}
        {{
            exceptions.raise_compiler_error(
                "dremio__edr_dateadd: unrecognized datepart '"
                ~ datepart
                ~ "'. Supported: year, quarter, month, week, day, hour, minute, second."
            )
        }}
    {% endif %}
{% endmacro %}
