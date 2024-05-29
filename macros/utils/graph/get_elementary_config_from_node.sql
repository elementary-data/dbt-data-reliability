{% macro get_elementary_config_from_node(node) %}
    {% set res = {} %}
    {% set node_config = node.get('config') %}
    {% if node_config %}
        {% do elementary.edr_log('Node config exists') %}
        {% set elementary_config = node.config.get('elementary') %}
        {% if elementary_config and elementary_config is mapping %}
            {% do elementary.edr_log('Elementary config in node config exists') %}
            {% do res.update(elementary_config) %}
        {% else %}
            {% do elementary.edr_log('Elementary config in node config does not exist') %}
        {% endif %}
        {% set config_meta = node.config.get('meta') %}
        {% if config_meta and config_meta is mapping %}
            {% set elementary_config = config_meta.get('elementary') %}
            {% if elementary_config and elementary_config is mapping %}
                {% do res.update(elementary_config) %}
            {% endif %}
        {% endif %}
    {% else %}
        {% do elementary.edr_log('Node config does not exist') %}
    {% endif %}
    {% set node_meta = node.get('meta') %}
    {% if node_meta and node_meta is mapping %}
        {% do elementary.edr_log('Node meta exists') %}
        {% set elementary_config = node_meta.get('elementary') %}
        {% if elementary_config and elementary_config is mapping %}
            {% do res.update(elementary_config) %}
        {% endif %}
    {% else %}
        {% do elementary.edr_log('Node meta does not exist') %}
    {% endif %}
    {% do elementary.edr_log('Print node:') %}
    {% do elementary.edr_log(node) %}
    {{ return(res) }}
{% endmacro %}