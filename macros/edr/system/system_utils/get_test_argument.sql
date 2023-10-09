{% macro get_test_argument(argument_name, value, model_graph_node) %}
  {% if value %}
    {{ return(value) }}
  {%- endif %}
  {%- if model_graph_node %}
    {% set elementary_config = elementary.get_elementary_config_from_node(model_graph_node) %}
    {% if elementary_config and elementary_config is mapping %}
        {%- set model_config_value = elementary_config.get(argument_name) %}
        {%- if model_config_value %}
            {{ return(model_config_value) }}
        {%- endif %}
    {% endif %}
  {% endif %}
  {% set config_value = elementary.get_config_var(argument_name) %}
  {% if config_value is defined %}
    {% do return(config_value) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}