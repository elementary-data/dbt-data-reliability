{% macro create_or_replace(temporary, relation, sql_query) %}
    {{ return(adapter.dispatch('create_or_replace', 'elementary')(temporary, relation, sql_query)) }}
{% endmacro %}

{# Snowflake and Bigquery #}
{% macro default__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
{% endmacro %}

{% macro redshift__create_or_replace(temporary, relation, sql_query) %}
    {% do dbt.drop_relation_if_exists(relation) %}
    {% do elementary.run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro postgres__create_or_replace(temporary, relation, sql_query) %}
    {% do elementary.run_query("BEGIN") %}
    {% do dbt.drop_relation_if_exists(relation) %}
    {% do elementary.run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
    {% do elementary.run_query("COMMIT") %}
{% endmacro %}

{% macro spark__create_or_replace(temporary, relation, sql_query) %}
    {% do dbt.drop_relation_if_exists(relation) %}
    {% do elementary.run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro athena__create_or_replace(temporary, relation, sql_query) %}
    {% do dbt.drop_relation_if_exists(relation) %}
    {% do elementary.run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
{% endmacro %}
