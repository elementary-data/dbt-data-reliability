{% macro get_test_argument(argument_name, value=none, model_node) %}
    {% if value %}
        {{- elementary.debug_log(argument_name ~ ' config from test: ' ~ value) }}
        {{ return(value) }}
    {%- else %}
        {% set elementary_config = elementary.get_elementary_config_from_node(model_node) %}
        {% if elementary_config and elementary_config is mapping %}
            {% set value = elementary_config.get(argument_name) %}
            {% if value %}
                {{- elementary.debug_log(argument_name ~ ' config from model: ' ~ value) }}
            {%- else %}
                {%- set value = elementary.get_config_var(argument_name) %}
                {{- elementary.debug_log(argument_name ~ ' not configured, using default: ' ~ value) }}
            {%- endif %}
        {% endif %}
    {% endif %}
    {{ return(value) }}
{% endmacro %}
