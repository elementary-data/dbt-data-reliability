{% macro run_anomaly_test(flattened_test, name, args) %}
  {% do elementary.init_elementary_graph() %}
  {% do print(args) %}
  {% do print(flattened_test) %}
  {% do print(name) %}
  {% do args.update({"model": ref(args["model"])}) %}
  {% do print(args) %}
  {% set test_macro_name = 'test_' ~ name %}
  {% set elemenatry_namespace = context["elementary"] %}
  {% if test_macro_name not in elemenatry_namespace %}
    {% do exceptions.raise_compiler_error("Could not find test " ~ test_macro_name) %}
  {% endif %}
  {% set test_macro = elemenatry_namespace[test_macro_name] %}
  {% do context.update({"elementary_force": true}) %}
  {% set test_sql = test_macro(**args) %}
  {% do print(test_sql) %}
  {% do context.update({"sql": test_sql}) %}
  {% set result_rows = elementary.get_anomaly_test_results_rows(flattened_test) %}
  {% do print(result_rows) %}
  {% do return(result_rows) %}
{% endmacro %}