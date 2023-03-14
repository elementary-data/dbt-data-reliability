{% macro limit(expression, sample_limit) -%}
    {{ return(adapter.dispatch('limit', 'elementary')(expression, sample_limit)) }}
{%- endmacro %}

{% macro default__limit(expression, sample_limit) %}

    {{ expression }} limit {{ sample_limit }}

{% endmacro %}

{% macro sqlserver__limit(expression, sample_limit) %}

    {{ expression | replace('select', 'select top(' ~ sample_limit ~ ')', 1) }}

{% endmacro %}