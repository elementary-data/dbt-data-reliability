{% macro lists_intersection(list_a, list_b) %}
    {% do return(set(list_a).intersection(set(list_b)) | list) %}
{% endmacro %}
