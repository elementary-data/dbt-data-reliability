{% macro get_elementary_test_table(test_relations, test_name, suffix) %}
    {% set test_table_name = elementary.table_name_with_suffix(test_name, suffix) %}
    {% set relation_keys = adapter.dispatch('get_relation_keys', 'elementary')() %}
    {% for test_relation in test_relations %}
        {% if test_relation['name'].lower() == test_table_name.lower() %}
            {{ return(api.Relation.create(
                database=test_relation[relation_keys['database']],
                schema=test_relation[relation_keys['schema']],
                identifier=test_relation[relation_keys['identifier']]
                ))
            }}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}


{% macro default__get_relation_keys() %}
    {{ return({
        'database': 'database',
        'schema': 'schema',
        'identifier': 'name'
    }) }}
{% endmacro %}

{% macro snowflake__get_relation_keys() %}
    {{ return({
        'database': 'database_name',
        'schema': 'schema_name',
        'identifier': 'name'
    }) }}
{% endmacro %}
