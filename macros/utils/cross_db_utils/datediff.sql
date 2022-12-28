{% macro datediff(first_date, second_date, datepart) %}
    {% if not execute %}
        {% do return(none) %}
    {% endif %}
    {% set macro = dbt.datediff or dbt_utils.datediff %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `datediff` macro.") }}
    {% endif %}
    {{ return(macro(first_date, second_date, datepart)) }}
{% endmacro %}
