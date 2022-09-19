{% test python(model, code_macro, packages) %}
  {% if not execute %}
    {% do return(none) %}
  {% endif %}

  {% if not code_macro %}
    {% do exceptions.raise_compiler_error('A `code_macro` must be provided to a Python test.') %}
  {% endif %}
  {% if not packages %}
    {% set packages = [] %}
  {% endif %}

  {% set model_relation = model.quote(false, false, false) %}
  {% set model_graph_node = context.model %}
  {% set elementary_database_name, elementary_schema_name = elementary.get_package_database_and_schema() %}
  {% set output_table = api.Relation.create(database=elementary_database_name, schema=elementary_schema_name,
    identifier='pytest_tmp__' ~ model_graph_node.alias).quote(false, false, false) %}

  {% do model_graph_node.update({'schema': model_relation.schema}) %}
  {% do model_graph_node.config.update({'packages': packages}) %}

  {% set user_py_code_macro = context[code_macro] %}
  {% if not user_py_code_macro %}
    {% do exceptions.raise_compiler_error('Unable to find the macro `' ~ code_macro ~ '`.') %}
  {% endif %}
  {% set user_py_code = user_py_code_macro() %}
  {% set compiled_py_code = adapter.dispatch('compile_py_code', 'elementary')(model_relation, user_py_code, output_table) %}

  {% do adapter.submit_python_job(model_graph_node, compiled_py_code) %}
  select * from {{ output_table }}
{% endtest %}
