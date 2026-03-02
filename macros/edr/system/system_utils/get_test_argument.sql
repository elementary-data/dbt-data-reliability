{% macro get_test_argument(argument_name, value, model_graph_node) %}
  {% if value %}
    {% do return(value) %}
  {%- endif %}
  {%- if model_graph_node %}
    {% set elementary_config = elementary.get_elementary_config_from_node(model_graph_node) %}
    {% if elementary_config and elementary_config is mapping %}
        {%- if argument_name in elementary_config %}
            {% do return(elementary_config.get(argument_name)) %}
        {%- endif %}
    {% endif %}
  {% endif %}
  {% set config_value = elementary.get_config_var(argument_name) %}
  {% if config_value is defined %}
    {% do return(config_value) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}