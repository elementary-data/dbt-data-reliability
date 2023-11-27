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

{% macro athena__edr_current_timestamp() -%}
    CURRENT_TIMESTAMP
{%- endmacro -%}

{% macro athena__edr_current_timestamp_in_utc() -%}
    cast(CURRENT_TIMESTAMP AT TIME ZONE 'utc' AS TIMESTAMP)
{%- endmacro -%}
