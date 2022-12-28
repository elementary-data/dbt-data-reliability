{% macro current_timestamp() -%}
    {% if not execute %}
        {% do return(none) %}
    {% endif %}
    {% set macro = dbt.current_timestamp_backcompat or dbt_utils.current_timestamp %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}


{% macro current_timestamp_in_utc() -%}
    {% if not execute %}
        {% do return(none) %}
    {% endif %}
    {% set macro = dbt.current_timestamp_in_utc_backcompat or dbt_utils.current_timestamp_in_utc %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `current_timestamp_in_utc` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{%- endmacro %}
