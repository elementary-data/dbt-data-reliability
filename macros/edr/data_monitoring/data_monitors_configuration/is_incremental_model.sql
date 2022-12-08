{% macro is_incremental_model(model_node, source_included=false) %}
  {% do return(
    (source_included and model_node.resource_type == "source")
    or
    (model_node.resource_type == "model" and model_node.config.materialized == "incremental")
  ) %}
{% endmacro %}
