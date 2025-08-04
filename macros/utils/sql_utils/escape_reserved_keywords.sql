{% macro escape_reserved_keywords(keyword) %}
    {% if elementary.is_reserved_keywords(keyword) %}
        {% do return(elementary.escape_keywords(keyword)) %}
    {% endif %}
    {% do return(keyword) %}
{% endmacro %}

{% macro is_reserved_keywords(keyword) %}
    {% do return(adapter.dispatch('is_reserved_keywords', 'elementary')(keyword)) %}
{% endmacro %}

{% macro default__is_reserved_keywords(keyword) %}
    {% do return(false) %}
{% endmacro %}

{% macro dremio__is_reserved_keywords(keyword) %}
    {% do return(keyword in ['filter', 'sql', 'timestamp']) %}
{% endmacro %}

{% macro escape_keywords(keyword) %}
    {% do return(adapter.dispatch('escape_keywords', 'elementary')(keyword)) %}
{% endmacro %}

{% macro default__escape_keywords(keyword) %}
    {% do return(keyword) %}
{% endmacro %}

{% macro dremio__escape_keywords(keyword) %}
    {% do return('"' ~ keyword ~ '"') %}
{% endmacro %}


