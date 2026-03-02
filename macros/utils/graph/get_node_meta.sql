{% macro get_node_meta(node_dict) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}

    {% set unified_meta = {} %}
    {% do unified_meta.update(config_meta_dict) %}
    {% do unified_meta.update(meta_dict) %}

    {% do return(unified_meta) %}
{% endmacro %}