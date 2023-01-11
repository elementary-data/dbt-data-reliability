{% macro set_cache(entry, val) %}
    {% if execute %}
        {% do graph.get("elementary", {}).update({entry: val}) %}
    {% endif %}
{% endmacro %}
