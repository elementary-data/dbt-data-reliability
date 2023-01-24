{% macro listagg(measure, delimiter_text="','", order_by_clause=none, limit_num=none) -%}
    {% set macro = dbt.listagg or dbt_utils.listagg %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `listagg` macro.") }}
    {% endif %}
    {{ return(macro(measure, delimiter_text, order_by_clause, limit_num)) }}
{%- endmacro %}
