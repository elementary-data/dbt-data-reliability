{% macro should_store_metrics(model_node) %}
  {% do return(
    model_node.resource_type == "source" or
    (model_node.resource_type == "model" and model_node.config.materialized == "incremental")
  ) %}
{% endmacro %}
