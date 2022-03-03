{% macro union_lists(list1, list2) %}
    {% set union_list = [] %}
    {% do union_list.extend(list1) %}
    {% do union_list.extend(list2) %}
    {{ return(union_list | unique | list) }}
{% endmacro %}
