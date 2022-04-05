{% macro column_quote(column_name) %}
    {{ adapter.dispatch('column_quote','elementary')(column_name) }}
{% endmacro %}

{%- macro default__column_quote(column_name) -%}
    {% if adapter.quote(column_name[1:-1]) == column_name %}
        {{ return(column_name) }}
    {% else %}
        {% set quoted_column = adapter.quote(column_name) %}
        {{ return(quoted_column) }}
    {% endif %}
{%- endmacro -%}

{%- macro snowflake__column_quote(column_name) -%}
    {% set upper_column_name = column_name | upper %}
    {% if adapter.quote(column_name[1:-1]) | upper == upper_column_name %}
        {{ return(upper_column_name) }}
    {% else %}
        {% set quoted_column = adapter.quote(column_name) %}
        {{ return(quoted_column | upper) }}
    {% endif %}
{%- endmacro -%}
