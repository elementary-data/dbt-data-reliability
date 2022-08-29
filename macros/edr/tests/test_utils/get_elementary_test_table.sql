{% macro get_elementary_test_table(test_relations, test_name, suffix) %}
    {% set test_table_name = elementary.table_name_with_suffix(test_name, suffix) %}
    {{ return(adapter.dispatch('get_elementary_test_table', 'elementary')(test_relations, test_table_name )) }}
{% endmacro %}


{% macro default__get_elementary_test_table(test_relations, test_table_name) %}
    {% for test_relation in test_relations %}
        {% if test_relation['name'].lower() == test_table_name.lower() %}
            {{ return(api.Relation.create(database=test_relation['database'], schema=test_relation['schema'], identifier=test_relation['name'])) }}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}

{% macro snowflake__get_elementary_test_table(test_relations, test_table_name) %}
    {% for test_relation in test_relations %}
        {% if test_relation['name'].lower() == test_table_name.lower() %}
            {{ return(api.Relation.create(database=test_relation['database_name'], schema=test_relation['schema_name'], identifier=test_relation['name'])) }}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}
