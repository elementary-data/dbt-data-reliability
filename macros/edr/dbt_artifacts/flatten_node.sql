{% macro flatten_node(node_dict) %}
  {% set resource_type = node_dict.get("resource_type") %}
  {% set flatten_func = {
    "model": elementary.flatten_model,
    "snapshot": elementary.flatten_model,
    "source": elementary.flatten_source,
    "seed": elementary.flatten_seed,
    "test": elementary.flatten_test,
    "group": elementary.flatten_group,
    "metric": elementary.flatten_metric,
    "exposure": elementary.flatten_exposure
  }.get(resource_type) %}

  {% if not flatten_func %}
    {% do exceptions.raise_compiler_error("Unknown resource type: " ~ resource_type) %}
  {% endif %}

  {% do return(flatten_func(node_dict)) %}
{% endmacro %}