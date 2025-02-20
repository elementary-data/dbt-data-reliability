{% macro create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% do return(adapter.dispatch("create_temp_table", "elementary")(database_name, schema_name, table_name, sql_query)) %}
{% endmacro %}

{% macro default__create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                               schema=schema_name,
                                                                               identifier=table_name,
                                                                               type='table') -%}
    {% set temp_table_relation = elementary.edr_make_temp_relation(temp_table_relation) %}
    {% if temp_table_exists %}
        {% do adapter.drop_relation(temp_table_relation) %}
        {% do elementary.run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% else %}
        {% do elementary.run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% endif %}
    {{ return(temp_table_relation) }}
{% endmacro %}

{% macro glue__create_temp_table(database_name, schema_name, table_name, sql_query) %}
    -- Apparently, inside glue, cannot query temporary tables
    -- Moroever, there is a problem inside edr_make_temp_relation, we need to enforce no suffix
    -- It is a problem because for the moment, it tries to get relation without suffix 
    -- and it puts none as suffix if not specified maybe the function is not well named
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                               schema=schema_name,
                                                                               identifier=table_name,
                                                                               type='table') -%}
    {% set temp_table_relation = elementary.edr_make_temp_relation(temp_table_relation, suffix="") %}
    
    {% if temp_table_exists %}
        {% do adapter.drop_relation(temp_table_relation) %}
        {% do elementary.run_query(dbt.create_table_as(False, temp_table_relation, sql_query)) %}
    {% else %}
        {% do elementary.run_query(dbt.create_table_as(False, temp_table_relation, sql_query)) %}
    {% endif %}
    {{ return(temp_table_relation) }}
{% endmacro %}

