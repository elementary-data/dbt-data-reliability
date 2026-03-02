{% macro get_node_meta(node_dict) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}

    {% set unified_meta = {} %}
    {%- set unified_meta = elementary.dict_merge(unified_meta, config_meta_dict) %}
    {%- set unified_meta = elementary.dict_merge(unified_meta, meta_dict) %}

    {% do return(unified_meta) %}
{% endmacro %}