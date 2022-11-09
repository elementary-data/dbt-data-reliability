{% macro get_elementary_test_table(test_name, table_type) %}
    {{ return(graph["elementary_test_tables"].get((test_name, table_type))) }}
{% endmacro %}
