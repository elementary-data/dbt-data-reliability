{% macro edr_current_timestamp() -%}
    {{ adapter.dispatch('edr_current_timestamp','elementary')() }}
{%- endmacro %}

{% macro default__edr_current_timestamp() -%}
    {% set macro = dbt.current_timestamp_backcompat or dbt_utils.current_timestamp %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}

{% macro databricks__edr_current_timestamp() -%}
    {% set macro = dbt.current_timestamp_backcompat or dbt_utils.current_timestamp %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}

{% macro spark__edr_current_timestamp() %}
    cast(current_timestamp() as timestamp)
{% endmacro %}

{% macro clickhouse__edr_current_timestamp() %}
    now()
{% endmacro %}

{% macro edr_current_timestamp_in_utc() -%}
    {{ adapter.dispatch('edr_current_timestamp_in_utc','elementary')() }}
{%- endmacro %}

{% macro default__edr_current_timestamp_in_utc() -%}
    {% set macro = dbt.current_timestamp_in_utc_backcompat or dbt_utils.current_timestamp_in_utc %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp_in_utc` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}

{% macro databricks__edr_current_timestamp_in_utc() -%}
    {% set macro = dbt.current_timestamp_in_utc_backcompat or dbt_utils.current_timestamp_in_utc %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp_in_utc` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}

{% macro spark__edr_current_timestamp_in_utc() %}
    cast(unix_timestamp() as timestamp)
{% endmacro %}

{% macro clickhouse__edr_current_timestamp_in_utc() %}
    now('UTC')
{% endmacro %}

{% macro athena__edr_current_timestamp() -%}
    CURRENT_TIMESTAMP
{%- endmacro -%}

{% macro athena__edr_current_timestamp_in_utc() -%}
    cast(CURRENT_TIMESTAMP AT TIME ZONE 'utc' AS TIMESTAMP)
{%- endmacro -%}

{% macro trino__edr_current_timestamp() -%}
    current_timestamp(6)
{%- endmacro -%}

{% macro trino__edr_current_timestamp_in_utc() -%}
    cast(current_timestamp at time zone 'UTC' as timestamp(6))
{%- endmacro -%}

{% macro dremio__edr_current_timestamp() -%}
    CURRENT_TIMESTAMP()
{%- endmacro -%}

{% macro dremio__edr_current_timestamp_in_utc() -%}
    -- Dremio CURRENT_TIMESTAMP() is always in UTC
    CURRENT_TIMESTAMP()
{%- endmacro -%}
