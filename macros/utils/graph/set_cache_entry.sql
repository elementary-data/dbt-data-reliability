{% macro set_cache_entry(entry, val) %}
    {% if execute %}
        {% do graph.setdefault("elementary_cache", {}) %}
        {% do graph["elementary_cache"].update({entry: val}) %}
    {% endif %}
{% endmacro %}