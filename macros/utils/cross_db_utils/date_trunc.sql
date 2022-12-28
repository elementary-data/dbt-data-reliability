{% macro date_trunc(datepart, date) %}
    {% set macro = dbt.date_trunc or dbt_utils.date_trunc %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `date_trunc` macro.") }}
    {% endif %}
    {{ return(macro(datepart, date)) }}
{% endmacro %}
