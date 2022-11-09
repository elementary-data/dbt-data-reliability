{% macro set_cache(entry, val) %}
    {% if execute %}
        {% do graph.setdefault("elementary", {}) %}
        {% do graph["elementary"].update({entry: val}) %}
    {% endif %}
{% endmacro %}
