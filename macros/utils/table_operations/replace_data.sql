{% macro replace_data(relation, sql_query) %}
    {{ return(adapter.dispatch('replace_data', 'elementary')(relation, sql_query)) }}
{% endmacro %}

{% macro replace_data_with_table_contents(relation, replacing_relation) %}
    {% do return(elementary.replace_data(relation, 'select * from {}'.format(replacing_relation))) %}
{% endmacro %}

{# Databricks / Spark (non-atomic implementation) #}
{% macro default__replace_data(relation, sql_query) %}
    {% do dbt.truncate_relation(relation) %}
    {% do elementary.insert_as_select(relation, sql_query) %}
{% endmacro %}

{% macro snowflake__replace_data(relation, sql_query) %}
    {% set query %}
        begin transaction;
        truncate table {{ relation }};
        insert into {{ relation }} {{ sql_query }};
        commit;
    {% endset %}
    {% do dbt.run_query(query) %}
{% endmacro %}

{% macro bigquery__replace_data(relation, sql_query) %}
    {% set query %}
        begin transaction;
        delete from {{ relation }} where true;   -- truncate not supported in BigQuery transactions
        insert into {{ relation }} {{ sql_query }};
        commit transaction;
    {% endset %}
    {% do dbt.run_query(query) %}
{% endmacro %}

{% macro redshift__replace_data(relation, sql_query) %}
    {% set query %}
        begin transaction;
        delete from {{ relation }};   -- truncate supported in Redshift transactions, but causes an immediate commit
        insert into {{ relation }} {{ sql_query }};
        commit;
    {% endset %}
    {% do dbt.run_query(query) %}
{% endmacro %}
