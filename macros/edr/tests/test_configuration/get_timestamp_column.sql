{% macro get_timestamp_column(timestamp_column, model_node) %}
    {% if timestamp_column %}
        {{ return(timestamp_column) }}
    {% else %}
        {% set elementary_config = elementary.get_elementary_config_from_node(model_node) %}
        {% if elementary_config and elementary_config is mapping %}
            {{ return(elementary_config.get('timestamp_column')) }}
        {% endif %}
    {% endif %}
    {{- return(none) -}}
{% endmacro %}