{% macro join_list(item_list, separator) %}
    {{ return(item_list | join(separator)) }}
{% endmacro %}
