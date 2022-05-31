{% macro safe_get_with_default(dict, key, default) %}
    {% set value = dict.get(key) %}
    {% if value is defined and value is not none %}
        {{ return(value) }}
    {% endif %}
    {% if default is defined %}
        {{ return(default) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
