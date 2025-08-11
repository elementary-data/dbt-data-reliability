{% macro create_or_replace(temporary, relation, sql_query) %}
    {{ return(adapter.dispatch('create_or_replace', 'elementary')(temporary, relation, sql_query)) }}
{% endmacro %}

{# Snowflake and Bigquery #}
{% macro default__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query) %}
{% endmacro %}

{% macro redshift__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query, drop_first=true, should_commit=true) %}
{% endmacro %}

{% macro postgres__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.run_query("BEGIN") %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query, drop_first=true) %}
    {% do elementary.run_query("COMMIT") %}
{% endmacro %}

{% macro spark__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query, drop_first=true, should_commit=true) %}
{% endmacro %}

{% macro athena__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query, drop_first=true) %}
{% endmacro %}

{% macro trino__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query, drop_first=true) %}
{% endmacro %}

{% macro clickhouse__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.edr_create_table_as(temporary, relation, sql_query, drop_first=true) %}
{% endmacro %}
