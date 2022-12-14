{% macro generate_json_schema_test(node_name, column_name) %}
    {% if target.type != 'snowflake' %}
      {% do print("JSON schema test generation is currently only supported for Snowflake (the test itself is also supported on BigQuery)") %}
      {% do return(none) %}
    {% endif %}

    {% set node = elementary.get_node_by_name(node_name) %}
    {% if node.resource_type not in ["source", "model"] %}
      {% do print("Only sources and models are supported for this macro, supplied node type: '{}'".format(node.resource_type)) %}
      {% do return(none) %}
    {% endif %}

    {% set node_relation = get_relation_from_node(node) %}
    {% if not elementary.column_exists_in_relation(node_relation, column_name) %}
      {% do print("Column '{}' does not exist in {} '{}'!".format(column_name, node.resource_type, node_name)) %}
      {% do return(none) %}
    {% endif %}

    {% set elementary_database_name, elementary_schema_name = elementary.get_package_database_and_schema() %}

    {% do node.config.update({"packages": ["genson"]}) %}
    {% do node.update({'database': elementary_database_name, 'schema': elementary_schema_name}) %}
    {% if node.resource_type == 'source' %}
        {% do node.update({'alias': "{}_{}".format(node.source_name, node.name)}) %}
    {% endif %}

    {% set output_table = api.Relation.create(database=elementary_database_name, schema=elementary_schema_name,
        identifier='json_schema_tmp__' ~ node.alias).quote(false, false, false) %}

    {% set gen_json_schema_func = elementary.generate_json_schema_py_func(column_name) %}
    {% set compiled_py_code = adapter.dispatch('compile_py_code', 'elementary')(node_relation, gen_json_schema_func,
                                                                                output_table, code_type='function') %}

    {% do elementary.run_python(node, compiled_py_code) %}
    {% set json_schema = elementary.result_value('select result from {}'.format(output_table)) %}
    {% if not json_schema %}
        {% do print("Not a valid JSON column: {}".format(column_name)) %}
        {% do return(none) %}
    {% endif %}

    {% set json_schema = fromjson(json_schema) %}
    {% do json_schema.pop('$schema', None) %}

    {% set testyaml %}
tests:
  - elementary.test_json_schema:
      column: {{ column_name }}
      json_schema:
        {{ toyaml(json_schema) | indent(8) }}
    {% endset %}

    {% do print("Please add the following test to your {} configuration:".format(node.resource_type)) %}
    {% do print(testyaml) %}
{% endmacro %}

{% macro generate_json_schema_py_func(column_name) %}
import json
import genson

def get_column_name_in_df(df, column):
    matching = [col for col in df.columns if col.lower() == column.lower()]
    if len(matching) == 0:
        raise Exception("No column with the name: {}".format(col))
    elif len(matching) > 1:
        # If there's more than one match, then case matters, so return as-is
        return column

    return matching[0]

def func(model_df, ref, session):
    model_df = model_df.toPandas()
    builder = genson.SchemaBuilder()
    column_name = get_column_name_in_df(model_df, "{{ column_name }}")
    for val in set(model_df[column_name]):
        if val == "" or val is None:
            continue
        try:
            builder.add_object(json.loads(val))
        except json.JSONDecodeError:
            # Not a valid json column, no schema
            return None
    return builder.to_schema()

{% endmacro %}
