{% macro current_timestamp() -%}
    {{ adapter.dispatch('current_timestamp','elementary')() }}
{%- endmacro %}

{% macro default__current_timestamp() -%}
    {% set macro = dbt.current_timestamp_backcompat or dbt_utils.current_timestamp %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}

{% macro spark__current_timestamp() %}
    current_timestamp()
{% endmacro %}


{% macro current_timestamp_in_utc() -%}
    {{ adapter.dispatch('current_timestamp_in_utc','elementary')() }}
{%- endmacro %}

{% macro default__current_timestamp_in_utc() -%}
    {% set macro = dbt.current_timestamp_in_utc_backcompat or dbt_utils.current_timestamp_in_utc %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp_in_utc` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}

{% macro spark__current_timestamp_in_utc() %}
    unix_timestamp()
{% endmacro %}