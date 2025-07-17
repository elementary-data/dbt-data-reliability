{% macro create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% do return(adapter.dispatch('create_temp_table','elementary')(database_name, schema_name, table_name, sql_query)) %}
{%- endmacro %}

{% macro default__create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                               schema=schema_name,
                                                                               identifier=table_name,
                                                                               type='table') -%}
    {% set temp_table_relation = elementary.make_temp_table_relation(temp_table_relation) %}
    {% do elementary.edr_create_table_as(True, temp_table_relation, sql_query, drop_first=temp_table_exists) %}
    {{ return(temp_table_relation) }}{% endmacro %}

{% macro snowflake__create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                               schema=schema_name,
                                                                               identifier=table_name,
                                                                               type='table') -%}
    {% set temp_table_relation = elementary.make_temp_table_relation(temp_table_relation) %}
    {% set create_query %}
        create or replace temporary table {{ temp_table_relation }} 
        as (
            {{ sql_query }}
        );
        
    {% endset %}

    {% do elementary.run_query(create_query) %}
    
    {{ return(temp_table_relation) }}
{% endmacro %}