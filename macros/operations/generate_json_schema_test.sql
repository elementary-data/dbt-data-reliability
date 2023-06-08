{% macro generate_json_schema_test(node_name, column_name) %}
    {% if target.type not in ['snowflake', 'bigquery'] %}
      {% do exceptions.raise_compiler_error("JSON schema test generation is not supported for target: {}".format(target.type)) %}
    {% endif %}

    {% set node = elementary.get_node_by_name(node_name) %}
    {% if node.resource_type not in ["source", "model"] %}
      {% do exceptions.raise_compiler_error("Only sources and models are supported for this macro, supplied node type: '{}'".format(node.resource_type)) %}
    {% endif %}

    {% set node_relation = get_relation_from_node(node) %}
    {% if not elementary.column_exists_in_relation(node_relation, column_name) %}
      {% do exceptions.raise_compiler_error("Column '{}' does not exist in {} '{}'!".format(column_name, node.resource_type, node_name)) %}
    {% endif %}

    {% set elementary_database_name, elementary_schema_name = elementary.get_package_database_and_schema() %}

    {% do node.config.update({"packages": ["genson"]}) %}
    {% do node.update({'database': elementary_database_name, 'schema': elementary_schema_name}) %}
    {% if node.resource_type == 'source' %}
      {# Source nodes don't have alias, and submit_python_job expects it #}
      {% do node.update({'alias': "jsonschemagen_{}_{}".format(node.source_name, node.name)}) %}
    {% endif %}

    {% set output_table = api.Relation.create(database=elementary_database_name, schema=elementary_schema_name,
        identifier='json_schema_tmp__' ~ node.alias).quote(false, false, false) %}

    {% set gen_json_schema_func = elementary.generate_json_schema_py_func(column_name) %}
    {% set node_relation = node_relation.quote(false, false, false) %}
    {% set compiled_py_code = adapter.dispatch('compile_py_code', 'elementary')(node_relation, gen_json_schema_func,
                                                                                output_table, code_type='function') %}

    {% do elementary.run_python(node, compiled_py_code) %}
    {% set json_schema = elementary.result_value('select result from {}'.format(output_table)) %}
    {% if json_schema == 'genson_not_installed' %}
      {% do exceptions.raise_compiler_error("The 'genson' python library is missing from your warehouse.\n\n"
         "This macro relies on the 'genson' python library for generating JSON schemas. Please follow dbt's instructions here: \n"
         "https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models#specific-data-warehouses\n"
         "regarding how to install python packages for a {} warehouse.".format(target.type)
      ) %}
    {% endif %}

    {% if not json_schema %}
        {% do exceptions.raise_compiler_error("Not a valid JSON column: {}".format(column_name)) %}
    {% endif %}

    {% set json_schema = fromjson(json_schema) %}
    {% do json_schema.pop('$schema', None) %}

    {% set testyaml %}
columns:
  - name: {{ column_name }}
    tests:
      - elementary.json_schema:
          {{ toyaml(json_schema) | indent(10) }}
    {% endset %}

    {% do print("Please add the following test to your {} configuration for the column {}:".format(node.resource_type, column_name)) %}
    {% do print(testyaml) %}
{% endmacro %}

{% macro generate_json_schema_py_func(column_name) %}
import json
try:
    import genson
except ImportError:
    genson = None

def get_column_name_in_df(df, column_name):
    matching = [col for col in df.columns if col.lower() == column_name.lower()]
    if len(matching) > 1:
        # Case matters, switch to case-sensitive match
        matching = [col for col in df.columns if col == column_name]

    if len(matching) == 0:
        raise Exception("No column with the name: {}".format(col))
    return matching[0]

def func(model_df, ref, session):
    if genson is None:
        return "genson_not_installed"

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
