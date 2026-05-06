{% macro _merge_elementary_from_meta(res, meta_dict) %}
    {% if meta_dict and meta_dict is mapping %}
        {% set elementary_config = meta_dict.get("elementary") %}
        {% if elementary_config and elementary_config is mapping %}
            {% do res.update(elementary_config) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro get_elementary_config_from_node(node) %}
    {% set res = {} %}
    {% set node_config = node.get("config") %}
    {% if node_config %}
        {# config.elementary is already the elementary config dict itself (not nested) #}
        {% set config_elementary = node.config.get("elementary") %}
        {% if config_elementary and config_elementary is mapping %}
            {% do res.update(config_elementary) %}
        {% endif %}
        {% do elementary._merge_elementary_from_meta(res, node.config.get("meta")) %}
    {% endif %}
    {% do elementary._merge_elementary_from_meta(res, node.get("source_meta")) %}
    {% do elementary._merge_elementary_from_meta(res, node.get("meta")) %}
    {{ return(res) }}
{% endmacro %}
