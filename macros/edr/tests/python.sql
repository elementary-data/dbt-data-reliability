{% test python(model, code_macro, macro_args, where_expression, detailed_test_output) %}
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

{% if detailed_test_output %}
  {% set detailed_output_table = api.Relation.create(database=elementary_database_name, schema=elementary_schema_name,
    identifier='elementary_detailed_output_' ~ test_node.alias).quote(false, false, false) %}
{% endif %}


  {# This affects where resources needed for python execution (e.g. stored procedures) are created.
     By default, dbt uses the audit schema (adds _dbt__test_audit to the model's schema).
     We prefer to change this behavior and use Elementary's database and schema instead (this also guarantees the test
     will work for sources).
     #}
  {% do test_node.update({'database': elementary_database_name, 'schema': elementary_schema_name}) %}

  {% do test_node.config.update(test_args) %}

  {% if code_macro is string %}
    {% set user_py_code_macro = context[code_macro] %}
  {% else %}
    {% set user_py_code_macro = code_macro %}
  {% endif %}

  {% if not user_py_code_macro %}
    {% do exceptions.raise_compiler_error('Unable to find the macro `' ~ code_macro ~ '`.') %}
  {% endif %}
  {% set user_py_code = user_py_code_macro(macro_args) %}
  {% set compiled_py_code = adapter.dispatch('compile_py_code', 'elementary')(model_relation, user_py_code,
                                                                              output_table, where_expression, detailed_output_table, code_type='test') %}

  {% do elementary.run_python(test_node, compiled_py_code) %}

  {% if detailed_test_output %}
  select fail_count, detailed_output_table from {{ output_table }}
  {% else %}
  select fail_count from {{ output_table }}
  {% endif %}
{% endtest %}
