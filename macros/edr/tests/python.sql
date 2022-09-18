{% test python(model, code_macro, packages, output_table) %}
  {% if not execute %}
    {% do return(none) %}
  {% endif %}

  {% if not code_macro %}
    {% do exceptions.raise_compiler_error('A `code_macro` must be provided to a Python test.') %}
  {% endif %}
  {% if not packages %}
    {% set packages = [] %}
  {% endif %}
  {% set elementary_database_name, elementary_schema_name = elementary.get_package_database_and_schema() %}
  {% set model_graph_node = context.model %}
  {% if not output_table %}
    {% set output_table = api.Relation.create(database=elementary_database_name, schema=elementary_schema_name,
      identifier='pytest__' ~ model_graph_node.alias).quote(false, false, false) %}
  {% endif %}

  {% do model_graph_node.update({'schema': model.schema}) %}
  {% do model_graph_node['config'].update({'packages': packages}) %}

  {% set user_py_code_macro = context[code_macro] %}
  {% if not user_py_code_macro %}
    {% do exceptions.raise_compiler_error('Unable to find the macro `' ~ code_macro ~ '`.') %}
  {% endif %}
  {% set user_py_code = user_py_code_macro() %}
  {% set compiled_py_code = elementary.compile_py_code(model_graph_node, user_py_code) %}
  {% set write_table_py_code = py_write_table(compiled_py_code, output_table) %}

  {% do adapter.submit_python_job(model_graph_node, write_table_py_code) %}
  select * from {{ output_table }}
{% endtest %}


{% macro compile_py_code(model_graph_node, py_code) %}
{{ py_code }}
{{ py_script_postfix(model_graph_node) }}
model = test
{% endmacro %}
