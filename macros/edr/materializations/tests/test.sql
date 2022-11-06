{% materialization test, default %}
    {% set ele_db, ele_schema = elementary.get_package_database_and_schema() %}
    {% set relation = api.Relation.create(database=ele_db, schema=ele_schema, identifier='test_result__{}'.format(model.alias), type='table') %}
    {% do elementary.create_or_replace(false, relation, sql) %}
    {% set get_test_result_sql = 'select * from {}'.format(relation) %}
    {% do context.update({'sql': get_test_result_sql}) %}
    {% set test_result = dbt.run_query(get_test_result_sql) %}
    {% do graph["elementary"]["test_results"].update({model.unique_id: test_result}) %}
    {{ return(dbt.materialization_test_default()) }}
{% endmaterialization %}
