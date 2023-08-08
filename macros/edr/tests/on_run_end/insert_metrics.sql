{% macro insert_metrics() %}
  {% set metrics = elementary.get_cache("tables").get("metrics").get("rows") %}
  {% set database_name, schema_name = elementary.get_package_database_and_schema() %}
  {%- set target_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier='data_monitoring_metrics') -%}
  {% if not target_relation %}
    {% do exceptions.warn("Couldn't find Elementary's models. Please run `dbt run -s elementary`.") %}
    {% do return(none) %}
  {% endif %}

  {{ elementary.file_log("Inserting {} metrics into {}.".format(metrics | length, target_relation)) }}
  {% do elementary.insert_rows(target_relation, metrics, should_commit=true) %}
{% endmacro %}
