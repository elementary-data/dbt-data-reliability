{% macro get_node_by_relation(model_relation) %}
  {% set matched_model_node = none %}

  {% if execute %}
    {# Get all models #}
    {% set models = graph.nodes.values()
        | selectattr("resource_type", "equalto", "model") %}

    {% for model in models %}
      {% if model.database == model_relation.database
          and model.schema == model_relation.schema
          and (
            model.name == model_relation.identifier
            or model.alias == model_relation.identifier
          ) %}
        {% set matched_model_node = model %}
        {{ return(matched_model_node) }}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ return(matched_model_node) }}
{% endmacro %}
