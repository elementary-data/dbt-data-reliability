{% macro run_anomaly_test(flattened_test, name, args) %}
  {% do flattened_test.update({"unique_id": elementary.get_test_unique_id()}) %}
  {% do elementary.init_elementary_graph() %}
  {% do args.update({"model": ref(args["model"])}) %}
  {% set test_macro_name = 'test_' ~ name %}
  {% set elemenatry_namespace = context["elementary"] %}
  {% if test_macro_name not in elemenatry_namespace %}
    {% do exceptions.raise_compiler_error("Could not find test " ~ test_macro_name) %}
  {% endif %}
  {% set test_macro = elemenatry_namespace[test_macro_name] %}
  {% do context.update({"elementary_force": true}) %}
  {# {{ debug() }} #}
  {% set test_sql = test_macro(**args) %}
  {% do print(test_sql) %}
  {% do context.update({"sql": test_sql}) %}
  {% set result_rows = elementary.get_anomaly_test_results_rows(flattened_test) %}
  {{ log("hey", true )}}
  {{ debug() }}
  {% do print(result_rows) %}
  {% do return(result_rows) %}
{% endmacro %}

{% macro check() %}
  {% set unique_id = model.get('unique_id') %}
  {# {{ debug() }} #}
  {% set ftest = {
      "test_params": {
        "timestamp_column": "timestamp"
      },
      "model_tags": "",
      "tags": ""
    } %}
  {% set args = {
      "model": "my_first_dbt_model",
      "timestamp_column": "timestamp",
      "time_bucket": {"period": "hour", "count": 1}
    } %}


  {{ elementary.run_anomaly_test(ftest, "freshness_anomalies", args) }}
{% endmacro %}