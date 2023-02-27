{% macro full_refresh() -%}
    {{ return(adapter.dispatch('full_refresh', 'elementary')()) }}
{%- endmacro %}

{% macro default__full_refresh() %}

    flags.FULL_REFRESH

{% endmacro %}

{% macro sqlserver__full_refresh() %}

    1 if flags.FULL_REFRESH else 0

{% endmacro %}