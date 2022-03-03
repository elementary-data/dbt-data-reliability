{% macro safe_get_with_default(dict, key, default) %}
    {% set value = dict.get(key) %}
    {% if not value %}
        {% set value = default %}
    {% endif %}
    {{ return(value) }}
{% endmacro %}
