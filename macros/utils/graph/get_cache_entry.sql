{% macro get_cache_entry(entry) %}
    {% if execute %}
        {{ return(graph.get("elementary_cache", {}).get(entry)) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
