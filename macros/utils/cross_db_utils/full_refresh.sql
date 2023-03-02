{% macro full_refresh() -%}
    {{ return(adapter.dispatch('full_refresh', 'elementary')()) }}
{%- endmacro %}

{% macro default__full_refresh() %}

    flags.FULL_REFRESH

{% endmacro %}

{% macro sqlserver__full_refresh() %}

    {% if flags.FULL_REFRESH %}
        {{ return(1) }}
    {% else %}
        {{ return(0) }}
    {% endif %}

{% endmacro %}