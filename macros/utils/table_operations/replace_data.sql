{% macro replace_data(relation, sql_query) %}
    {{ return(adapter.dispatch('replace_data', 'elementary')(relation, sql_query)) }}
{% endmacro %}

{% macro replace_data_with_table_contents(relation, replacing_relation) %}
    {% do return(elementary.replace_data(relation, 'select * from {}'.format(replacing_relation))) %}
{% endmacro %}

{# Default - simply replace the table #}
{% macro default__replace_data(relation, sql_query) %}
    {% do dbt.run_query(dbt.create_table_as(False, relation, sql_query)) %}
{% endmacro %}

{# In postgres/redshift we do not want to replace the table, because that will cause views without
   late binding to be deleted. So instead we atomically replace the data in a transaction #}
{% macro postgres__replace_data(relation, sql_query) %}
    {% set query %}
        begin transaction;
        delete from {{ relation }};   -- truncate supported in Redshift transactions, but causes an immediate commit
        insert into {{ relation }} {{ sql_query }};
        commit;
    {% endset %}
    {% do dbt.run_query(query) %}
{% endmacro %}
