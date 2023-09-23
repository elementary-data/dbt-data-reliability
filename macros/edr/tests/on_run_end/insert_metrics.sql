{% macro insert_metrics() %}
  {% set metrics = elementary.get_cache("tables").get("metrics").get("rows") %}
  {%- set target_relation = elementary.get_elementary_relation('data_monitoring_metrics') -%}
  {% if not target_relation %}
    {% do exceptions.warn("Couldn't find Elementary's models in `" ~ elementary.target_database() ~ "." ~ target.schema ~ "`. Please run `dbt run -s elementary --target " ~ target.name ~ "`.") %}
    {% do return(none) %}
  {% endif %}

  {{ elementary.file_log("Inserting {} metrics into {}.".format(metrics | length, target_relation)) }}
  {% do elementary.insert_rows(target_relation, metrics, should_commit=true) %}
{% endmacro %}
