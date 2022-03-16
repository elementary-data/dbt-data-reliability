{% macro column_quote(column_name) %}
    {{ adapter.dispatch('column_quote','elementary')(column_name) }}
{% endmacro %}

{%- macro default__column_quote(column_name) -%}
    {%- set quoted_column = '"' ~ column_name | upper ~ '"' -%}
    {{- return(quoted_column) -}}
{%- endmacro -%}

{% macro bigquery__column_quote(column_name) %}
    {%- set quoted_column = '`' ~ column_name ~ '`' -%}
    {{- return(quoted_column) -}}
{% endmacro %}