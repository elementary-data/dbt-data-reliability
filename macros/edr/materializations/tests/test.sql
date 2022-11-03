{%- materialization test, default -%}
    {% set ele_db, ele_schema = elementary.get_package_database_and_schema() %}
    {% set relation = api.Relation.create(database=ele_db, schema=ele_schema, identifier=model.alias, type='table') -%} %}
    {% do dbt.run_query(elementary.create_or_replace(false, relation, sql)) %}
    {% do context.update({'sql': 'select * from {}'.format(relation)}) %}
    {{ return(dbt.materialization_test_default()) }}
{%- endmaterialization -%}
