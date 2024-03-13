{% macro clean_elementary_test_tables() %}
    {% set test_table_relations = [] %}
    {% set temp_test_table_relations_map = elementary.get_cache("temp_test_table_relations_map") %}
    {% if temp_test_table_relations_map %}
        {% for test_entry in temp_test_table_relations_map.values() %}
            {% for test_relation in test_entry.values() %}
                {% do test_table_relations.append(test_relation) %}
            {% endfor %}
        {% endfor %}

        {% do elementary.file_log("Deleting temporary Elementary test tables: {}".format(test_table_relations)) %}
        {% set queries = elementary.get_clean_elementary_test_tables_queries(test_table_relations) %}
        {% for query in queries %}
            {% do elementary.run_query(query) %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(adapter.dispatch("get_clean_elementary_test_tables_queries", "elementary")(test_table_relations)) %}
{% endmacro %}

{% macro default__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(elementary.get_transaction_clean_elementary_test_tables_queries(test_table_relations)) %}
{% endmacro %}

{% macro bigquery__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(elementary.get_transactionless_clean_elementary_test_tables_queries(test_table_relations)) %}
{% endmacro %}

{% macro spark__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(elementary.get_transactionless_clean_elementary_test_tables_queries(test_table_relations)) %}
{% endmacro %}

{% macro athena__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% set queries = [] %}
    {% for test_relation in test_table_relations %}
        {% do queries.append("DROP TABLE IF EXISTS {}".format(test_relation.render_pure())) %}
    {% endfor %}
    {% do return(queries) %}
{% endmacro %}

{% macro trino__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% set queries = [] %}
    {% for test_relation in test_table_relations %}
        {% do queries.append("DROP TABLE IF EXISTS {}".format(test_relation)) %}
    {% endfor %}
    {% do return(queries) %}
{% endmacro %}

{% macro get_transaction_clean_elementary_test_tables_queries(test_table_relations) %}
    {% set query %}
        BEGIN TRANSACTION;
        {% for test_relation in test_table_relations %}
            DROP TABLE IF EXISTS {{ test_relation }};
        {% endfor %}
        COMMIT;
    {% endset %}
    {% do return([query]) %}
{% endmacro %}

{% macro get_transactionless_clean_elementary_test_tables_queries(test_table_relations) %}
    {% set queries = [] %}
    {% for test_relation in test_table_relations %}
        {% do queries.append("DROP TABLE IF EXISTS {}".format(test_relation)) %}
    {% endfor %}
    {% do return(queries) %}
{% endmacro %}
