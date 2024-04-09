{% macro insert_metrics() %}
  {% set metrics = elementary.get_cache("tables").get("metrics").get("rows") %}
  {%- set target_relation = elementary.get_elementary_relation('data_monitoring_metrics') -%}
  {% if not target_relation %}
    {% do elementary.warn_missing_elementary_models() %}
    {% do return(none) %}
  {% endif %}

  {{ elementary.file_log("Inserting {} metrics into {}.".format(metrics | length, target_relation)) }}
  {% do elementary.insert_rows(target_relation, metrics, should_commit=true, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
{% endmacro %}
