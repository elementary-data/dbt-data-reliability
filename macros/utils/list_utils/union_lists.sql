{% macro union_lists(list1, list2) %}
    {% set union_list = [] %}
    {%- if list1 and list1 is iterable %}
        {% do union_list.extend(list1) %}
    {%- endif %}
    {%- if list2 and list2 is iterable %}
        {% do union_list.extend(list2) %}
    {%- endif %}
    {{ return(union_list | unique | list) }}
{% endmacro %}
