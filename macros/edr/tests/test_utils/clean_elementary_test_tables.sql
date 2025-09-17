{% macro clean_elementary_test_tables() %}
    {% set test_table_relations = [] %}
    {% set temp_test_table_relations_map = elementary.get_cache("temp_test_table_relations_map") %}
    {% if temp_test_table_relations_map %}
        {% for test_entry in temp_test_table_relations_map.values() %}
            {% for test_relation in test_entry.values() %}
                {% do test_table_relations.append(test_relation) %}
            {% endfor %}
        {% endfor %}

        {# Extra entry-point to clean up tables before dropping the relation #}
        {% do elementary.clean_up_tables(test_table_relations) %}

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

{% macro clickhouse__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(elementary.get_transactionless_clean_elementary_test_tables_queries(test_table_relations)) %}
{% endmacro %}

{% macro athena__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {# Athena requires explicit backtick quoting for DROP TABLE statements to avoid parsing errors #}
    {% set queries = [] %}
    {% for test_relation in test_table_relations %}
        {% set escaped_database = test_relation.database | replace('`', '``') if test_relation.database else none %}
        {% set escaped_schema = test_relation.schema | replace('`', '``') %}
        {% set escaped_identifier = test_relation.identifier | replace('`', '``') %}
        {% if test_relation.database %}
            {% set quoted_relation = "`{}`.`{}`.`{}`".format(escaped_database, escaped_schema, escaped_identifier) %}
        {% else %}
            {% set quoted_relation = "`{}`.`{}`".format(escaped_schema, escaped_identifier) %}
        {% endif %}
        {% do queries.append("DROP TABLE IF EXISTS {}".format(quoted_relation)) %}
    {% endfor %}
    {% do return(queries) %}
{% endmacro %}

{% macro trino__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(elementary.get_transactionless_clean_elementary_test_tables_queries(test_table_relations)) %}
{% endmacro %}

{% macro dremio__get_clean_elementary_test_tables_queries(test_table_relations) %}
    {% do return(elementary.get_transactionless_clean_elementary_test_tables_queries(test_table_relations)) %}
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
