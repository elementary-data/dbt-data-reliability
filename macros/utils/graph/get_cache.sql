{% macro get_cache(entry, default=none) %}
    {% if execute %}
        {{ return(graph.get("elementary", {}).get(entry, default)) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
