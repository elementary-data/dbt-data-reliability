{% macro dateadd(datepart, interval, from_date_or_timestamp) %}
    {% if not execute %}
        {% do return(none) %}
    {% endif %}
    {% set macro = dbt.dateadd or dbt_utils.dateadd %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `dateadd` macro.") }}
    {% endif %}
    {{ return(macro(datepart, interval, from_date_or_timestamp)) }}
{% endmacro %}
