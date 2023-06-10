{% macro get_config_argument(argument_name, value, model_node, test_node) %}
    {% if value %}
        {{ return(value) }}
    {%- endif %}
    {%- if test_node %}
        {%- set test_config_value = elementary.get_argument_from_config_and_meta(argument_name, test_node) %}
        {%- if test_config_value %}
            {{ return(test_config_value) }}
        {%- endif %}
    {%- endif %}
    {%- if model_node %}
        {%- set model_config_value = elementary.get_argument_from_config_and_meta(argument_name, model_node) %}
        {%- if model_config_value %}
            {{ return(model_config_value) }}
        {%- endif %}
    {% endif %}
    {%- if elementary.get_config_var(argument_name) %}
        {{ return(elementary.get_config_var(argument_name)) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro get_argument_from_config_and_meta(argument_name, node_dict) %}
    {%- set config_dict, meta_dict = elementary.get_node_config_and_meta(node_dict) %}
    {% if config_dict and config_dict is mapping %}
        {%- set argument_value = config_dict.get(argument_name) %}
        {%- if argument_value %}
            {{ return(argument_value) }}
        {%- endif %}
    {% endif %}
    {% if meta_dict and meta_dict is mapping %}
        {%- set argument_value = meta_dict.get(argument_name) %}
        {%- if argument_value %}
            {{ return(argument_value) }}
        {%- endif %}
    {% endif %}
    {{ return(none) }}
{% endmacro %}