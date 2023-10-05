{% macro run_anomaly_test(flattened_test, name, args) %}
  {% do flattened_test.update({"unique_id": elementary.get_test_unique_id()}) %}
  {% do graph.nodes.update({flattened_test["parent_model_unique_id"]: {"identifier": flattened_test["parent_model_unique_id"]}}) %}
  {% do elementary.init_elementary_graph() %}
  {% do args.update({"model": ref(args["model"])}) %}
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
  {{ log("hey", true )}}
  {% do print(result_rows) %}
  {% do elementary.clean_elementary_test_tables() %}
  {% do return(result_rows) %}
{% endmacro %}

{% macro check() %}
  {% set unique_id = model.get('unique_id') %}
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


{% macro upload_test_results(test_results) %}
  {% set elementary_test_results_relation = ref("elementary", "elementary_test_results") %}
  {% do elementary.insert_rows(elementary_test_results_relation, elementary_test_results, should_commit=True) %}
{% endmacro %}

{% macro upload_dbt_testss(tests) %}
  {% set dbt_tests_relation = ref("elementary", "dbt_tests") %}
  {% do elementary.insert_rows(dbt_tests_relation, tests, should_commit=True) %}
{% endmacro %}


{% macro run_and_commit(sql) %}
  {% do print(render(sql)) %}
  {% call statement("blabla", fetch_result=False, auto_begin=True) %}
    {{ render(sql) }}
  {% endcall %}
  {% do adapter.commit() %}
{% endmacro %}