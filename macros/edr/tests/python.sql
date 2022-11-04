{% test python(model, code_macro, macro_args) %}
  {{ config(fail_calc = 'fail_count') }}

  {% if not execute %}
    {% do return(none) %}
  {% endif %}

  {% if not code_macro %}
    {% do exceptions.raise_compiler_error('A `code_macro` must be provided to a Python test.') %}
  {% endif %}
  {% if not macro_args %}
    {% set macro_args = {} %}
  {% endif %}

  {% set test_args = kwargs %}
  {% set test_node = context.model %}
  {% set model_relation = model.quote(false, false, false) %}
  {% set elementary_database_name, elementary_schema_name = elementary.get_package_database_and_schema() %}
  {% set output_table = api.Relation.create(database=elementary_database_name, schema=elementary_schema_name,
    identifier='pytest_tmp__' ~ test_node.alias).quote(false, false, false) %}

  {# Test nodes schemas are overwritten with __test_audit. #}
  {% do test_node.update({'schema': model_relation.schema}) %}
  {% do test_node.config.update(test_args) %}

  {% set user_py_code_macro = context[code_macro] %}
  {% if not user_py_code_macro %}
    {% do exceptions.raise_compiler_error('Unable to find the macro `' ~ code_macro ~ '`.') %}
  {% endif %}
  {% set user_py_code = user_py_code_macro(macro_args) %}
  {% set compiled_py_code = adapter.dispatch('compile_py_code', 'elementary')(model_relation, user_py_code, output_table) %}

  {% do elementary.run_python(test_node, compiled_py_code) %}
  select fail_count from {{ output_table }}
{% endtest %}
