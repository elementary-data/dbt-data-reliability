{# Some RDBMS do not support the typical stddev function name #}
{% macro stddev(column_name) -%}
    {{ return(adapter.dispatch('stddev', 'elementary')(column_name)) }}
{%- endmacro %}

{# Snowflake and Redshift #}
{% macro default__stddev(column_name) -%}
    stddev({{ column_name }})
{%- endmacro %}

{# SQL Server #}
{% macro sqlserver__stddev(column_name) -%}
    stdev({{ column_name }})
{%- endmacro %}