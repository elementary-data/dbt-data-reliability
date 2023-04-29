{% macro get_test_argument(argument_name, value=none, model_node=none) %}
  {% if value %}
    {{ return(value) }}
  {%- elif model_node %}
    {% set elementary_config = elementary.get_elementary_config_from_node(model_node) %}
    {% if elementary_config and elementary_config is mapping %}
        {{ return(elementary_config.get(argument_name)) }}
    {% endif %}
  {% else %}
    {{ return(elementary.get_config_var(argument_name)) }}
  {% endif %}
{% endmacro %}
