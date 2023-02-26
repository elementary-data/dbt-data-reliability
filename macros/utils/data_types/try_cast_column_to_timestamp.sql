{% macro try_cast_column_to_timestamp(table_relation, timestamp_column) %}
    {{ return(adapter.dispatch('try_cast_column_to_timestamp', 'elementary')(table_relation, timestamp_column)) }}
{%- endmacro %}

{% macro default__try_cast_column_to_timestamp(table_relation, timestamp_column) %}
    {# We try casting for Snowflake, Bigquery and Databricks as these support safe cast and the query will not fail if the cast fails #}
    {%- set query %}
        select {{ elementary.edr_safe_cast(timestamp_column, elementary.edr_type_timestamp()) }} as timestamp_column
        from {{ table_relation }}
        where {{ timestamp_column }} is not null
        limit 1
    {%- endset %}

    {%- set result = elementary.result_value(query) %}
    {%- if result is not none %}
        {{ return(true) }}
    {%- endif %}
    {{ return(false) }}

{% endmacro %}

{% macro postgres__try_cast_column_to_timestamp(table_relation, timestamp_column) %}
    {{ return(false) }}
{% endmacro %}
