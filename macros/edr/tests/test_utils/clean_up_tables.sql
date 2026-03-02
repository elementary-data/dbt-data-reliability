{% macro clean_up_tables(relations) %}
    {{ return(adapter.dispatch('clean_up_tables', 'elementary')(relations)) }}
{% endmacro %}

{% macro default__clean_up_tables(relations) %}
    {# Default implementation does nothing #}
{% endmacro %}

{% macro athena__clean_up_tables(relations) %}
    {% for relation in relations %}
        {% do adapter.clean_up_table(relation) %}
    {% endfor %}
{% endmacro %}