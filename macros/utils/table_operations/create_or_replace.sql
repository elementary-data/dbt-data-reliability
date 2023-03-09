{% macro create_or_replace(temporary, relation, sql_query) %}
    {{ return(adapter.dispatch('create_or_replace', 'elementary')(temporary, relation, sql_query)) }}
{% endmacro %}

{# Snowflake and Bigquery #}
{% macro default__create_or_replace(temporary, relation, sql_query) %}
    {% do run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
{% endmacro %}

{% macro redshift__create_or_replace(temporary, relation, sql_query) %}
    {% do dbt.drop_relation_if_exists(relation) %}
    {% do run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro postgres__create_or_replace(temporary, relation, sql_query) %}
    {% do run_query("BEGIN") %}
    {% do dbt.drop_relation_if_exists(relation) %}
    {% do run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
    {% do run_query("COMMIT") %}
{% endmacro %}

{% macro spark__create_or_replace(temporary, relation, sql_query) %}
    {% do dbt.drop_relation_if_exists(relation) %}
    {% do run_query(dbt.create_table_as(temporary, relation, sql_query)) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro sqlserver__create_or_replace(temporary, relation, sql_query) %}
    
    {% set table_query = 'WITH cte AS ({}) SELECT * INTO {} FROM cte'.format(sql_query, relation) %}
    {% set drop_if_relation_exist_list = dbt.get_or_create_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier, type=relation.type) -%}

    {% if drop_if_relation_exist_list[0] %}
        {% call statement('drop_relation', auto_begin=False) -%}
            DROP {{ drop_if_relation_exist_list[1].type }} {{ drop_if_relation_exist_list[1] }}
        {%- endcall %}
    {% endif %}

    {% do elementary.run_query(table_query) %}

{% endmacro %}
