{% macro test_athena_quoting_with_database() %}
    {% set mock_relations = [
        api.Relation.create(database='test_db', schema='test_schema', identifier='test_table')
    ] %}
    {% set queries = elementary.athena__get_clean_elementary_test_tables_queries(mock_relations) %}
    {% for query in queries %}
        {{ log(query, info=True) }}
    {% endfor %}
    {{ return('') }}
{% endmacro %}

{% macro test_athena_quoting_without_database() %}
    {% set mock_relations = [
        api.Relation.create(schema='test_schema', identifier='test_table')
    ] %}
    {% set queries = elementary.athena__get_clean_elementary_test_tables_queries(mock_relations) %}
    {% for query in queries %}
        {{ log(query, info=True) }}
    {% endfor %}
    {{ return('') }}
{% endmacro %}

{% macro test_athena_quoting_special_chars() %}
    {% set mock_relations = [
        api.Relation.create(database='test-db', schema='test_schema', identifier='test-table')
    ] %}
    {% set queries = elementary.athena__get_clean_elementary_test_tables_queries(mock_relations) %}
    {% for query in queries %}
        {{ log(query, info=True) }}
    {% endfor %}
    {{ return('') }}
{% endmacro %}

{% macro test_athena_quoting_normal_names() %}
    {% set mock_relations = [
        api.Relation.create(database='normal_db', schema='normal_schema', identifier='normal_table')
    ] %}
    {% set queries = elementary.athena__get_clean_elementary_test_tables_queries(mock_relations) %}
    {% for query in queries %}
        {{ log(query, info=True) }}
    {% endfor %}
    {{ return('') }}
{% endmacro %}
