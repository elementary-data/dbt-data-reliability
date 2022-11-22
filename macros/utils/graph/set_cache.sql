{% macro set_cache(entry, val) %}
    {% if execute %}
        {% do graph["elementary"].update({entry: val}) %}
    {% endif %}
{% endmacro %}
