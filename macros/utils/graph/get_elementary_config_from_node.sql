{% macro get_elementary_config_from_node(node) %}
    {% set res = {} %}
    {% set node_config = node.get('config') %}
    {% if node_config %}
        {% set elementary_config = node.config.get('elementary') %}
        {% if elementary_config and elementary_config is mapping %}
            {% do res.update(elementary_config) %}
        {% endif %}
        {% set config_meta = node.config.get('meta') %}
        {% if config_meta and config_meta is mapping %}
            {% set elementary_config = config_meta.get('elementary') %}
            {% if elementary_config and elementary_config is mapping %}
                {% do res.update(elementary_config) %}
            {% endif %}
        {% endif %}
    {% endif %}
    {% set node_meta = node.get('meta') %}
    {% if node_meta and node_meta is mapping %}
        {% set elementary_config = node_meta.get('elementary') %}
        {% if elementary_config and elementary_config is mapping %}
            {% do res.update(elementary_config) %}
        {% endif %}
    {% endif %}
    {{ return(res) }}
{% endmacro %}