{% macro get_elementary_test_table(test_name, table_type) %}
    {% if execute %}
        {{ return(graph.get("elementary_test_tables", {}).get((test_name, table_type))) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
