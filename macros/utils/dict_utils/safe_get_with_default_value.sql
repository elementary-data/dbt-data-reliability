{% macro safe_get_with_default(dict, key, default) %}
    {% set value = dict.get(key) %}
    {% if value %}
        {{ return(value) }}
    {% endif %}
    {% if default %}
        {{ return(default) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
