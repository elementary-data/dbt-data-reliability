{% macro escape_select(column_names) %}
    {% do return(adapter.dispatch('escape_select', 'elementary')(column_names)) %}
{% endmacro %}

{% macro default__escape_select(column_names) %}
    {% do return(column_names | join(',')) %}
{% endmacro %}

{% macro redshift__escape_select(column_names) %}
    {% do return('\"' + column_names | join('\", \"') + '\"') %}
{% endmacro %}

{% macro dremio__escape_select(column_names) %}
    {% do return('\"' + column_names | join('\", \"') + '\"') %}
{% endmacro %}