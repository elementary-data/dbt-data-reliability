{% macro merge_lists(list_of_lists) %}

    {%- set new_list = [] %}

    {%- for list in list_of_lists %}
        {%- for i in list %}
            {{ new_list.append(i) }}
        {%- endfor %}
    {%- endfor %}

    {{ return(new_list) }}

{% endmacro %}