{% macro set_cache(entry, val) %}
    {% set _elem = graph.setdefault("elementary", {}) %}
    {% do elementary.dict_set(_elem, entry, val) %}
{% endmacro %}

{% macro get_cache(entry, default=none) %}
    {% do return(graph.setdefault("elementary", {}).get(entry, default)) %}
{% endmacro %}

{% macro setdefault_cache(entry, default=none) %}
    {% do return(graph.setdefault("elementary", {}).setdefault(entry, default)) %}
{% endmacro %}
