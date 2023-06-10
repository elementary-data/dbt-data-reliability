{% macro get_node_config_and_meta(node_dict) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}
    {% set meta_dict = {} %}
    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% do meta_dict.update(elementary.safe_get_with_default(node_dict, 'meta', {})) %}
    {% do meta_dict.update(config_meta_dict) %}
    {{ return(config_dict, meta_dict) }}
{% endmacro %}