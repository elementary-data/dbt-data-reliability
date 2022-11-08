{% macro create_elementary_test_table(database_name, schema_name, test_name, table_type, sql_query) %}
    {% set temp_table_name = elementary.table_name_with_suffix(test_name, "__" ~ table_type) %}
    {{ elementary.debug_log(table_type ~ ' table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_table_name) }}

    {% set _, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                               schema=schema_name,
                                                                               identifier=temp_table_name,
                                                                               type='table') -%}

    {# Save the test table to the graph for easy access later #}
    {% do graph.setdefault("elementary_test_tables", {}) %}
    {% do graph["elementary_test_tables"].update({(test_name, table_type): temp_table_relation}) %}

    {# Create the table if it doesn't exist #}
    {%- do elementary.create_or_replace(False, temp_table_relation, sql_query) %}

    {{ return(temp_table_relation) }}
{% endmacro %}