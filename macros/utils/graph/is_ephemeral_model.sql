{% macro is_ephemeral_model(model_node) %}
    {% do return(
      model_node.resource_type == "model" and model_node.config.materialized == "ephemeral"
    ) %}
{% endmacro %}
