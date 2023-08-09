{% macro create_temp_table(database_name, schema_name, table_name, sql_query) %}
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