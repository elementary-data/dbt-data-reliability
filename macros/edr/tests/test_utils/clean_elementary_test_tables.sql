{% macro clean_elementary_test_tables() %}
    {% set test_table_relations = [] %}
    {% set temp_test_table_relations_map = elementary.get_cache("temp_test_table_relations_map") %}
    {% for test_entry in temp_test_table_relations_map.values() %}
        {% for test_relation in test_entry.values() %}
            {% do test_table_relations.append(test_relation) %}
        {% endfor %}
    {% endfor %}

    {% do elementary.file_log("Deleting temporary Elementary test tables: {}".format(test_table_relations)) %}
    {% set query %}
        DROP TABLE IF EXISTS {{ test_table_relations | join(", ") }} CASCADE;
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}
