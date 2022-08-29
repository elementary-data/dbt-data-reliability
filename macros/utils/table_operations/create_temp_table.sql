{% macro create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                               schema=schema_name,
                                                                               identifier=table_name,
                                                                               type='table') -%}
    {% if temp_table_exists %}
        {% do adapter.drop_relation(temp_table_relation) %}
        {% do run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% else %}
        {% do run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% endif %}
    {{ return(temp_table_relation) }}
{% endmacro %}